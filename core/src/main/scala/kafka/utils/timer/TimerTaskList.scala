/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.utils.timer

import java.util.concurrent.{Delayed, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Time

import scala.math._

/**
 * TimerTaskList 实现了双向循环链表。它定义了一个 Root 节点，同时还定义了两个字段：
 * taskCounter，用于标识当前这个链表中的总定时任务数；
 * expiration，表示这个链表所在 Bucket 的过期时间戳。
 * 每个 Bucket 对应于手表表盘上的一格。它有起始时间和结束时间，因而也就有时间间隔的概念，即“结束时间 - 起始时间 = 时间间隔”。
 * 同一层的 Bucket 的时间间隔都是一样的。只有当前时间越过了 Bucket 的起始时间，这个 Bucket 才算是过期。而这里的起始时间，就是代码中 expiration 字段的值。
 */
@threadsafe
private[timer] class TimerTaskList(taskCounter: AtomicInteger) extends Delayed {

  // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
  // root.next points to the head
  // root.prev points to the tail
  private[this] val root = new TimerTaskEntry(null, -1)
  root.next = root
  root.prev = root

  private[this] val expiration = new AtomicLong(-1L)

  // Set the bucket's expiration time
  // Returns true if the expiration time is changed
  /** 代码使用了 AtomicLong 的 CAS 方法 getAndSet 原子性地设置了过期时间戳，之后将新过期时间戳和旧值进行比较，看看是否不同，然后返回结果。
   * 这里为什么要比较新旧值是否不同呢？这是因为，目前 Kafka 使用一个 DelayQueue 统一管理所有的 Bucket，也就是 TimerTaskList 对象。
   * 随着时钟不断向前推进，原有 Bucket 会不断地过期，然后失效。当这些 Bucket 失效后，源码会重用这些 Bucket。
   * 重用的方式就是重新设置 Bucket 的过期时间，并把它们加回到 DelayQueue 中。
   * 这里进行比较的目的，就是用来判断这个 Bucket 是否要被插入到 DelayQueue。
  */
  def setExpiration(expirationMs: Long): Boolean = {
    expiration.getAndSet(expirationMs) != expirationMs
  }

  // Get the bucket's expiration time
  def getExpiration: Long = expiration.get

  // Apply the supplied function to each of tasks in this list
  def foreach(f: (TimerTask)=>Unit): Unit = {
    synchronized {
      var entry = root.next
      while (entry ne root) {
        val nextEntry = entry.next

        if (!entry.cancelled) f(entry.timerTask)

        entry = nextEntry
      }
    }
  }

  // Add a timer task entry to this list
  def add(timerTaskEntry: TimerTaskEntry): Unit = {
    var done = false
    while (!done) {
      // Remove the timer task entry if it is already in any other list
      // We do this outside of the sync block below to avoid deadlocking.
      // We may retry until timerTaskEntry.list becomes null.
      // 在添加之前尝试移除该定时任务，保证该任务没有在其他链表中
      timerTaskEntry.remove()

      synchronized {
        timerTaskEntry.synchronized {
          if (timerTaskEntry.list == null) {
            // put the timer task entry to the end of the list. (root.prev points to the tail entry)
            val tail = root.prev
            timerTaskEntry.next = root
            timerTaskEntry.prev = tail
            timerTaskEntry.list = this
            tail.next = timerTaskEntry
            root.prev = timerTaskEntry
            taskCounter.incrementAndGet()
            done = true
          }
        }
      }
    }
  }

  // Remove the specified timer task entry from this list
  def remove(timerTaskEntry: TimerTaskEntry): Unit = {
    synchronized {
      timerTaskEntry.synchronized {
        if (timerTaskEntry.list eq this) {
          timerTaskEntry.next.prev = timerTaskEntry.prev
          timerTaskEntry.prev.next = timerTaskEntry.next
          timerTaskEntry.next = null
          timerTaskEntry.prev = null
          timerTaskEntry.list = null
          taskCounter.decrementAndGet()
        }
      }
    }
  }

  // Remove all task entries and apply the supplied function to each of them
  def flush(f: TimerTaskEntry => Unit): Unit = {
    synchronized {
      // 找到链表第一个元素
      var head = root.next
      // 开始遍历链表
      while (head ne root) {
        // 移除遍历到的链表元素
        remove(head)
        // 执行传入参数f的逻辑
        f(head)
        head = root.next
      }
      // 清空过期时间设置
      expiration.set(-1L)
    }
  }

  def getDelay(unit: TimeUnit): Long = {
    unit.convert(max(getExpiration - Time.SYSTEM.hiResClockMs, 0), TimeUnit.MILLISECONDS)
  }

  def compareTo(d: Delayed): Int = {
    val other = d.asInstanceOf[TimerTaskList]
    java.lang.Long.compare(getExpiration, other.getExpiration)
  }

}
/**
 * 该类定义了 TimerTask 类字段，用来指定定时任务，同时还封装了一个过期时间戳字段，这个字段值定义了定时任务的过期时间。
 * 举个例子，假设有个 PRODUCE 请求在当前时间 1 点钟被发送到 Broker，超时时间是 30 秒，那么，该请求必须在 1 点 30 秒之前完成，
 * 否则将被视为超时。这里的 1 点 30 秒，就是 expirationMs 值。
 * 除了 TimerTask 类字段，该类还定义了 3 个字段：list、next 和 prev。
 * 它们分别对应于 Bucket 链表实例以及自身的 next、prev 指针。
 * 注意，list 字段是 volatile 型的，这是因为，Kafka 的延时请求可能会被其他线程从一个链表搬移到另一个链表中，
 * 因此，为了保证必要的内存可见性，代码声明 list 为 volatile。
 */
private[timer] class TimerTaskEntry(val timerTask: TimerTask, val expirationMs: Long) extends Ordered[TimerTaskEntry] {

  @volatile
  var list: TimerTaskList = _ // 绑定的Bucket链表实例
  var next: TimerTaskEntry = _ // next指针
  var prev: TimerTaskEntry = _ // prev指针

  // if this timerTask is already held by an existing timer task entry,
  // setTimerTaskEntry will remove it.
  // 关联给定的定时任务
  if (timerTask != null) timerTask.setTimerTaskEntry(this)
  // 关联定时任务是否已经被取消了
  def cancelled: Boolean = {
    timerTask.getTimerTaskEntry != this
  }

  /**
   * 从Bucket链表中移除自己
   * remove 的逻辑是将 TimerTask 自身从双向链表中移除掉，因此，代码调用了 TimerTaskList 的 remove 方法来做这件事。
   * 那这里就有一个问题：“怎么算真正移除掉呢？”其实，这是根据“TimerTaskEntry 的 list 是否为空”来判断的。
   * 一旦置空了该字段，这个 TimerTaskEntry 实例就不再属于任何一个链表了。从这个角度来看，置空就相当于移除的效果。
   */
  def remove(): Unit = {
    var currentList = list
    // If remove is called when another thread is moving the entry from a task entry list to another,
    // this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
    // In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
    while (currentList != null) {
      currentList.remove(this)
      currentList = list
    }
  }

  override def compare(that: TimerTaskEntry): Int = {
    java.lang.Long.compare(expirationMs, that.expirationMs)
  }
}

