/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.io.contention;

import java.util.Locale;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.SupportsCatalogTransactions;
import org.apache.iceberg.catalog.TableIdentifier;

/** A catalog's transactions ({@link SupportsCatalogTransactions}) as the drivers run them. */
public final class CatalogTxns implements TxnSupport {
  private final SupportsCatalogTransactions catalog;

  private CatalogTxns(SupportsCatalogTransactions catalog) {
    this.catalog = catalog;
  }

  public static TxnSupport of(SupportsCatalogTransactions catalog) {
    return new CatalogTxns(catalog);
  }

  @Override
  public Txn begin(String isolation) {
    CatalogTransaction transaction =
        catalog.createTransaction(
            CatalogTransaction.IsolationLevel.valueOf(isolation.toUpperCase(Locale.ROOT)));
    return new Txn() {
      @Override
      public Table table(TableIdentifier id) {
        return transaction.asCatalog().loadTable(id);
      }

      @Override
      public void commit() {
        transaction.commitTransaction();
      }
    };
  }
}
