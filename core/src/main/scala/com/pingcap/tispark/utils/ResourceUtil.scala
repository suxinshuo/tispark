package com.pingcap.tispark.utils

object ResourceUtil {

  /**
   * 资源管理工具 - 自动关闭 AutoCloseable 资源
   *
   * @param resource 资源创建表达式（by-name 参数，延迟初始化）
   * @param f        使用资源的函数
   * @tparam R 资源类型（必须是 AutoCloseable 子类）
   * @tparam A 返回类型
   * @return 函数执行结果
   */
  def using[R <: AutoCloseable, A](resource: => R)(f: R => A): A = {
    var r: R = null.asInstanceOf[R]
    try {
      // 初始化资源
      r = resource
      // 使用资源
      f(r)
    } finally {
      if (r != null) {
        try r.close()
        catch {
          case e: Exception =>
            // 记录关闭异常（不抛出，避免掩盖主异常）
            System.err.println(s"资源关闭失败: ${e.getMessage}")
        }
      }
    }
  }

}
