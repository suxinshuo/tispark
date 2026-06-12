/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.plans.logical

import com.pingcap.tispark.auth.TiAuthorization
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace

object TiBasicLogicalPlan {
  def verifyAuthorizationRule(
      logicalPlan: LogicalPlan,
      tiAuthorization: Option[TiAuthorization]): LogicalPlan = {
    logicalPlan match {
      // Spark 3.4+ changed SetCatalogAndNamespace into a UnaryCommand whose child is
      // the resolved namespace (ResolvedDBObjectName was removed in favor of
      // ResolvedIdentifier; the USE-namespace case always resolves to ResolvedNamespace).
      case st @ SetCatalogAndNamespace(child) =>
        child match {
          case rn: ResolvedNamespace =>
            if (rn.catalog.name().equals("tidb_catalog")) {
              rn.namespace.foreach(TiAuthorization.authorizeForSetDatabase(_, tiAuthorization))
            }
            st
          case _ =>
            st
        }
    }
  }

}
