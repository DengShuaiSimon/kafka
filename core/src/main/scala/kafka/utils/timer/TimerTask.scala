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

/**
 * TimerTask 是一个 Scala 接口（Trait）。
 * 每个 TimerTask 都有一个 delayMs 字段，表示这个定时任务的超时时间。
 * 通常来说，这就是客户端参数 request.timeout.ms 的值。这个类还绑定了一个 timerTaskEntry 字段，
 * 因为，每个定时任务都要知道，它存放在哪个 Bucket 链表下的哪个链表元素上。
 */
trait TimerTask extends Runnable {

  val delayMs: Long // timestamp in millisecond 通常是request.timeout.ms参数值
  // 每个TimerTask实例关联一个TimerTaskEntry
  // 每个定时任务需要知道它在哪个Bucket链表下的哪个链表元素上
  private[this] var timerTaskEntry: TimerTaskEntry = _
  // 取消定时任务，原理就是将关联的timerTaskEntry置空
  def cancel(): Unit = {
    synchronized {
      if (timerTaskEntry != null) timerTaskEntry.remove()
      timerTaskEntry = null
    }
  }
  // 关联timerTaskEntry，原理是给timerTaskEntry字段赋值
  private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
    synchronized {
      // if this timerTask is already held by an existing timer task entry,
      // we will remove such an entry first.
      if (timerTaskEntry != null && timerTaskEntry != entry)
        timerTaskEntry.remove()

      timerTaskEntry = entry
    }
  }
  // 获取关联的timerTaskEntry实例
  private[timer] def getTimerTaskEntry: TimerTaskEntry = timerTaskEntry

}
