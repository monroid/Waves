package com.wavesplatform.database

import com.google.common.io.ByteStreams.{newDataOutput, newDataInput}
import com.google.common.primitives.{Ints, Longs}
import com.wavesplatform.state2.{AssetDescription, AssetInfo, ByteStr, LeaseBalance, VolumeAndFee}
import org.iq80.leveldb.{DB, ReadOptions}
import scalikejdbc.using
import scorex.account.{Address, Alias}
import scorex.block.Block
import scorex.transaction.assets.exchange.ExchangeTransaction
import scorex.transaction.assets.{IssueTransaction, ReissueTransaction}
import scorex.transaction.lease.{LeaseCancelTransaction, LeaseTransaction}
import scorex.transaction.{Transaction, TransactionParser}

import scala.collection.mutable.ArrayBuffer

/** The following namespaces are used:
  *
  * address -> waves balance history[]
  * (H, address) -> waves balance
  * address -> lease balance history[]
  * (H, address) -> lease balance
  * address -> asset ids[]
  * (address, asset id) -> asset balance history[]
  * (H, address, asset ID) -> asset balance
  * tx id -> (height, tx bytes)
  * H -> changed addresses[]
  * H -> (address, asset id)[]
  * H -> block
  * H -> txs[]
  *
  */
object LevelDBWriter {
  trait Key[V] {
    def keyBytes: Array[Byte]
    def parse(bytes: Array[Byte]): V
    def encode(v: V): Array[Byte]
  }

  object Key {
    def apply[V](key: Array[Byte], parser: Array[Byte] => V, encoder: V => Array[Byte]): Key[V] = new Key[V] {
      override def keyBytes = key
      override def parse(bytes: Array[Byte]) = parser(bytes)
      override def encode(v: V) = encoder(v)
    }

    def opt[V](key: Array[Byte], parser: Array[Byte] => V, encoder: V => Array[Byte]): Key[Option[V]] =
      apply[Option[V]](key, Option(_).map(parser), _.fold[Array[Byte]](null)(encoder))
  }

  object k {
    private def h(prefix: Int, height: Int): Array[Byte] = {
      val ndo = newDataOutput(8)
      ndo.write(prefix)
      ndo.write(height)
      ndo.toByteArray
    }

    private def address(prefix: Int, address: Address) = {
      val ndo = newDataOutput(4 + address.bytes.arr.length)
      ndo.write(prefix)
      ndo.write(address.bytes.arr)
      ndo.toByteArray
    }

    private def addressWithH(prefix: Int, height: Int, address: Address): Array[Byte] = {
      val ndo = newDataOutput(8 + address.bytes.arr.length)
      ndo.write(prefix)
      ndo.write(height)
      ndo.write(address.bytes.arr)
      ndo.toByteArray
    }

    private def writeIntSeq(values: Seq[Int]): Array[Byte] = {
      val ndo = newDataOutput()
      ndo.writeInt(values.length)
      values.foreach(ndo.writeInt)
      ndo.toByteArray
    }

    private def readIntSeq(data: Array[Byte]): Seq[Int] = {
      val ndi = newDataInput(data)
      (1 to ndi.readInt()).map(_ => ndi.readInt())
    }

    val height: Key[Int] =
      Key[Int](Array(0, 0, 0, 1), Option(_).fold(0)(Ints.fromByteArray), Ints.toByteArray)

    def score(height: Int) =
      Key[BigInt](h(2, height), BigInt.apply, _.toByteArray)

    def blockAt(height: Int): Key[Option[Block]] =
      Key.opt[Block](h(3, height), Block.parseBytes(_).get, _.bytes())
    def heightBySignature(blockId: ByteStr): Key[Option[Int]] = ???

    def wavesBalanceHistory(address: Address): Key[Seq[Int]] = Key(address(4, address), readIntSeq, writeIntSeq)

    def wavesBalance(height: Int, address: Address): Key[Long] =
      Key(addressWithH(5, height, address), Option(_).fold(0L)(Longs.fromByteArray), Longs.toByteArray)

    def assetList(address: Address): Key[Set[ByteStr]] = ???
    def assetBalanceHistory(address: Address, assetId: ByteStr): Key[Seq[Int]] =
      Key(address(4, address), readIntSeq, writeIntSeq)
    def assetBalance(height: Int, address: Address, assetId: ByteStr): Key[Long] = ???

    def assetInfoHistory(assetId: ByteStr): Key[Seq[Int]] = ???
    def assetInfo(height: Int, assetId: ByteStr): Key[AssetInfo] = ???
    def assetDescription(assetId: ByteStr): Key[Option[AssetDescription]] = ???

    def leaseBalanceHistory(address: Address): Key[Seq[Int]] = Key(address(4, address), readIntSeq, writeIntSeq)
    def leaseBalance(height: Int, address: Address): Key[LeaseBalance] = ???

    def leaseStateHistory(leaseId: ByteStr): Key[Seq[Int]] = ???

    def filledVolumeAndFeeHistory(orderId: ByteStr): Key[Seq[Int]] = ???
    def filledVolumeAndFee(height: Int, orderId: ByteStr): Key[VolumeAndFee] = ???

    def transactionInfo(txId: ByteStr): Key[Option[(Int, Option[Transaction])]] = ???
    def transactionExists(txId: ByteStr): Key[Boolean] = ???

    def addressTransactionHistory(address: Address): Key[Seq[Int]] = ???
    def addressTransactionIds(height: Int, address: Address): Key[Seq[ByteStr]] = ???

    def changedAddresses(height: Int): Key[Seq[Address]] = ???
    def transactionIdsAtHeight(height: Int): Key[Seq[ByteStr]] = ???
  }

  class ReadOnlyDB(db: DB, readOptions: ReadOptions) {
    def get[V](key: Key[V]): V = key.parse(db.get(key.keyBytes))
  }

  class RW(db: DB) extends AutoCloseable {
    private val batch = db.createWriteBatch()
    private val snapshot = db.getSnapshot
    private val readOptions = new ReadOptions().snapshot(snapshot)

    def get[V](key: Key[V]): V = key.parse(db.get(key.keyBytes, readOptions))

    def put[V](key: Key[V], value: V): Unit = batch.put(key.keyBytes, key.encode(value))

    def delete(key: Array[Byte]): Unit = batch.delete(key)

    def delete[V](key: Key[V]): Unit = batch.delete(key.keyBytes)

    override def close() = {
      try { db.write(batch) }
      finally {
        batch.close()
        snapshot.close()
      }
    }
  }

  private def loadAssetInfo(db: ReadOnlyDB, assetId: ByteStr) = {
    db.get(k.assetInfoHistory(assetId)).headOption.map(h => db.get(k.assetInfo(h, assetId)))
  }
}

class LevelDBWriter(writableDB: DB) extends Caches {
  import LevelDBWriter._

  private def readOnly[A](f: ReadOnlyDB => A): A = using(writableDB.getSnapshot) { s =>
    f(new ReadOnlyDB(writableDB, new ReadOptions().snapshot(s)))
  }

  private def readWrite[A](f: RW => A): A = using(new RW(writableDB)) { rw => f(rw) }

  override protected def loadHeight(): Int = readOnly(_.get(k.height))

  override protected def loadScore(): BigInt = readOnly { db => db.get(k.score(db.get(k.height))) }

  override protected def loadLastBlock(): Option[Block] = readOnly { db => db.get(k.blockAt(db.get(k.height))) }

  override protected def loadWavesBalance(address: Address): Long = readOnly { db =>
    db.get(k.wavesBalanceHistory(address)).headOption.fold(0L)(h => db.get(k.wavesBalance(h, address)))
  }

  override protected def loadLeaseBalance(address: Address): LeaseBalance = readOnly { db =>
    db.get(k.leaseBalanceHistory(address)).headOption.fold(LeaseBalance.empty)(h => db.get(k.leaseBalance(h, address)))
  }

  override protected def loadAssetBalance(address: Address): Map[ByteStr, Long] = readOnly { db =>
    (for {
      assetId <- db.get(k.assetList(address))
      h <- db.get(k.assetBalanceHistory(address, assetId)).headOption
    } yield assetId -> db.get(k.assetBalance(h, address, assetId))).toMap
  }

  override protected def loadAssetInfo(assetId: ByteStr): Option[AssetInfo] = readOnly(LevelDBWriter.loadAssetInfo(_, assetId))

  override protected def loadAssetDescription(assetId: ByteStr): Option[AssetDescription] = readOnly { db =>
    db.get(k.transactionInfo(assetId)) match {
      case Some((_, Some(i: IssueTransaction))) =>
        val reissuable = LevelDBWriter.loadAssetInfo(db, assetId).map(_.isReissuable)
        Some(AssetDescription(i.sender, i.name, i.decimals, reissuable.getOrElse(i.reissuable)))
      case _ => None
    }
  }

  override protected def loadVolumeAndFee(orderId: ByteStr): VolumeAndFee = readOnly { db =>
    db.get(k.filledVolumeAndFeeHistory(orderId)).headOption.fold(VolumeAndFee.empty)(h => db.get(k.filledVolumeAndFee(h, orderId)))
  }

  private def updateHistory(rw: RW, key: Key[Seq[Int]], threshold: Int, kf: Int => Key[_]): Seq[Array[Byte]] = {
    val (c1, c2) = rw.get(key).partition(_ >= threshold)
    rw.put(key, (height +: c1) ++ c2.headOption)
    c2.tail.map(kf(_).keyBytes)
  }

  override protected def doAppend(block: Block,
                                  wavesBalances: Map[Address, Long], /**/
                                  assetBalances: Map[Address, Map[ByteStr, Long]],
                                  leaseBalances: Map[Address, LeaseBalance], /**/
                                  leaseStates: Map[ByteStr, Boolean],
                                  transactions: Map[ByteStr, (Transaction, Set[Address])],
                                  reissuedAssets: Map[ByteStr, AssetInfo],
                                  filledQuantity: Map[ByteStr, VolumeAndFee]/**/): Unit = readWrite { rw =>
    val expiredKeys = new ArrayBuffer[Array[Byte]]

    rw.put(k.height, height)
    rw.put(k.blockAt(height), block)

    val threshold = height - 2000

    for ((address, balance) <- wavesBalances) {
      rw.put(k.wavesBalance(height, address), balance)
      expiredKeys ++= updateHistory(rw, k.wavesBalanceHistory(address), threshold, h => k.wavesBalance(h, address))
    }

    for ((address, leaseBalance) <- leaseBalances) {
      rw.put(k.leaseBalance(height, address), leaseBalance)
      expiredKeys ++= updateHistory(rw, k.leaseBalanceHistory(address), threshold, k.leaseBalance(_, address))
    }

    for ((orderId, volumeAndFee) <- filledQuantity) {
      rw.put(k.filledVolumeAndFee(height, orderId), volumeAndFee)
      expiredKeys ++= updateHistory(rw, k.filledVolumeAndFeeHistory(orderId), threshold, k.filledVolumeAndFee(_, orderId))
    }

    val changedAssetBalances = Set.newBuilder[(Address, ByteStr)]
    for ((address, assets) <- assetBalances) {
      rw.put(k.assetList(address), rw.get(k.assetList(address)) ++ assets.keySet)
      for ((assetId, balance) <- assets) {
        changedAssetBalances += address -> assetId
        rw.put(k.assetBalance(height, address, assetId), balance)
        expiredKeys ++= updateHistory(rw, k.assetBalanceHistory(address, assetId), threshold, k.assetBalance(_, address, assetId))
      }
    }

    for ((id, (tx, _)) <- transactions) {
      val txToSave = tx match {
        case (_: IssueTransaction | _: LeaseTransaction) => Some(tx)
        case _ => None
      }

      rw.put(k.transactionInfo(id), Some((height, txToSave)))
    }

    expiredKeys.foreach(rw.delete)
  }

  override protected def doRollback(targetBlockId: ByteStr): Seq[Block] = readWrite { rw =>
    rw.get(k.heightBySignature(targetBlockId)).fold(Seq.empty[Int])(_ to height).flatMap { h =>
      val blockAtHeight = rw.get(k.blockAt(h))
      for (address <- rw.get(k.changedAddresses(h))) {
        for (assetId <- rw.get(k.assetList(address))) {
          rw.delete(k.assetBalance(h, address, assetId))
          val historyKey = k.assetBalanceHistory(address, assetId)
          rw.put(historyKey, rw.get(historyKey).filterNot(_ == h))
        }

        rw.delete(k.wavesBalance(h, address))
        val wbh = k.wavesBalanceHistory(address)
        rw.put(wbh, rw.get(wbh).filterNot(_ == h))

        rw.delete(k.leaseBalance(h, address))
        val lbh = k.leaseBalanceHistory(address)
        rw.put(lbh, rw.get(lbh).filterNot(_ == h))
      }

      for (txId <- rw.get(k.transactionIdsAtHeight(h))) {
        rw.get(k.transactionInfo(txId)) match {
          case Some((_, Some(i: IssueTransaction))) =>
          case Some((_, Some(r: ReissueTransaction))) =>
          case Some((_, Some(c: LeaseCancelTransaction))) =>
          case Some((_, Some(l: LeaseTransaction))) =>
          case Some((_, Some(x: ExchangeTransaction))) =>
        }
      }

      blockAtHeight
    }
  }

  override def transactionInfo(id: ByteStr) = readOnly(db => db.get(k.transactionInfo(id)))

  override def addressTransactions(address: Address, types: Set[TransactionParser.TransactionType.Value], from: Int, count: Int) = ???

  override def paymentTransactionIdByHash(hash: ByteStr) = ???

  override def aliasesOfAddress(a: Address) = ???

  override def resolveAlias(a: Alias) = ???

  override def leaseDetails(leaseId: ByteStr) = ???

  override def balanceSnapshots(address: Address, from: Int, to: Int) = ???

  override def activeLeases = ???

  override def leaseOverflows = ???

  override def leasesOf(address: Address) = ???

  override def nonZeroLeaseBalances = ???

  override def scoreOf(blockId: ByteStr) = ???

  override def blockHeaderAndSize(height: Int) = ???

  override def blockHeaderAndSize(blockId: ByteStr) = ???

  override def blockBytes(height: Int) = ???

  override def blockBytes(blockId: ByteStr) = ???

  override def heightOf(blockId: ByteStr) = readOnly(_.get(k.heightBySignature(blockId)))

  override def lastBlockIds(howMany: Int) = ???

  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int) = ???

  override def parent(ofBlock: Block, back: Int) = ???

  override def approvedFeatures() = ???

  override def activatedFeatures() = ???

  override def featureVotes(height: Int) = ???

  override def status = ???
}
