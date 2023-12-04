# Data Migrator
## Architecture
| SOA + Domain 기반 + MVC & Humble Pattern
```mermaid
---
title: Pyoniverse Dashboard Architecture
---
C4Component
    title Pyoniverse Dashboard Architecture
    Container_Boundary(migration_boundary, "Data Migration") {
        Component(migrator, "Migrator", "", "Migrate from documentDB/rDB to rDB/documentDB")
        ComponentDb(tmp_storage, "S3", "Dump/Load Data Storage")
        Rel(document_db, tmp_storage, "Dump")
        Rel(rdb, tmp_storage, "Dump")
        Rel(tmp_storage, migrator, "Load & Convert format")
        Rel(migrator, document_db, "Update Data")
        Rel(migrator, rdb, "Update Data")
        UpdateRelStyle(document_db, tmp_storage, $textColor="red", $lineColor="green")
        UpdateRelStyle(rdb, tmp_storage, $textColor="red", $lineColor="green")
        UpdateRelStyle(tmp_storage, migrator, $textColor="red", $lineColor="green")
        UpdateRelStyle(migrator, document_db, $textColor="red", $lineColor="green")
        UpdateRelStyle(migrator, rdb, $textColor="red", $lineColor="green")
    }
    Container_Boundary(web_boundary, "Dashboard Web Application") {
        ComponentDb(rdb, "RDB", "MariaDB", "Dashboard DB")
    }
    Container_Boundary(service_boundary, "Pyoniverse Application") {
        ComponentDb(document_db, "DocumentDB", "MongoDB", "Service DB")
    }
```
