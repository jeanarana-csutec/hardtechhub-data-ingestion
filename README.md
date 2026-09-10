# HardTech Hub — Data Ingestion Pipeline

Este repositorio contiene la parte de **Data Science / Analytics** del proyecto HardTech Hub (curso CS2032 - Cloud Computing).

Incluye 3 contenedores en Python que extraen el 100% de los registros de las bases de datos del backend (PostgreSQL, MySQL, DynamoDB) y los suben a un bucket S3, listos para catalogarse con AWS Glue y consultarse con AWS Athena.

## Repositorio del backend

El código de los microservicios (Identity, Catalog, Order, Compatibility, Analytics) y sus bases de datos vive en:
 https://github.com/SebaU12/HardTechHub

Este repositorio (`hardtechhub-data-ingestion`) es independiente y se conecta a esas bases de datos por red, usando las variables del `.env`.

---

## 1. Estructura del repositorio

```
hardtechhub-data-ingestion/
├── docker-compose.yml
├── env.example
└── data-pipeline/
    ├── ingest-catalog/     # PostgreSQL (catalog-service) -> S3
    ├── ingest-identity/    # DynamoDB (identity-service) -> S3
    └── ingest-order/       # MySQL (order-service) -> S3
```

Cada carpeta de `data-pipeline/` tiene su propio `extract.py`, `Dockerfile` y `requirements.txt`.

---

## 2. Qué hace cada contenedor

| Contenedor | Fuente | Tablas extraídas | Formato subido a S3 |
|---|---|---|---|
| `ingest-catalog` | PostgreSQL (`hardtech_catalog`) | `brands`, `categories`, `products` | CSV |
| `ingest-identity` | DynamoDB (tabla `users`) | `users` | JSON Lines |
| `ingest-order` | MySQL (`hardtech_orders`) | `orders`, `order_items` | CSV |

Cada contenedor hace un *pull* del 100% de los registros (`SELECT *` / `scan()`) y sube el resultado a S3, particionado por fecha:
```
s3://<bucket>/db-extracts/<origen>/<tabla>/year=YYYY/month=MM/day=DD/<tabla>_HHMMSS.csv
```

Por defecto cada contenedor corre **una sola vez** y termina (pull único, como pide el enunciado). Si se quiere correr en loop periódico, se puede setear `INTERVAL_SECONDS` (en segundos) como variable de entorno.

---

## 3. Cómo correr el pipeline

### 3.1 Configurar variables de entorno

```bash
git clone https://github.com/jeanarana-csutec/hardtechhub-data-ingestion.git
cd hardtechhub-data-ingestion
cp env.example .env
nano .env
```

Completar en `.env`:

```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...          # obligatorio si se usa AWS Academy Learner Lab
AWS_DEFAULT_REGION=us-east-1

S3_BUCKET_REAL=hardtechhub-bucket   # o el nombre del bucket que se use

POSTGRES_HOST=<IP o host de la VM de bases de datos>
POSTGRES_PORT=5432
POSTGRES_DB=hardtech_catalog
POSTGRES_USER=hardtech
POSTGRES_PASSWORD=hardtech

MYSQL_HOST=<IP o host de la VM de bases de datos>
MYSQL_PORT=3306
MYSQL_DATABASE=hardtech_orders
MYSQL_USER=hardtech
MYSQL_PASSWORD=hardtech

DYNAMODB_ENDPOINT=              # dejar VACIO para usar DynamoDB real de AWS
DYNAMODB_REGION=us-east-1
USERS_TABLE=users
```

⚠️ **Nunca subir el archivo `.env` a GitHub** — contiene credenciales.

⚠️ Si se usa **AWS Academy Learner Lab**, las credenciales (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) expiran cada pocas horas y hay que refrescarlas desde el botón "AWS Details" del Lab cada vez que se reinicie la sesión.

### 3.2 Levantar los contenedores

```bash
docker compose up -d --build
docker logs -f <nombre-del-contenedor>   # ej: hardtechhub-data-ingestion-ingest-catalog-1
```

Cada contenedor termina solo tras subir sus archivos. Verificar en S3:
```bash
aws s3 ls s3://<bucket>/db-extracts/ --recursive
```

---

## 4. Cómo recrear el bucket S3

1. Consola de AWS → **S3** → **Create bucket**.
2. Nombre único (ej. `hardtechhub-bucket`).
3. Región: la misma que se use en `AWS_DEFAULT_REGION`.
4. Dejar "Block all public access" activado (default).
5. Create bucket.

No se necesita crear ninguna carpeta interna a mano — los contenedores las crean automáticamente al subir los archivos.

---

## 5. Cómo recrear el catálogo de AWS Glue

### 5.1 Crear la base de datos

**Glue → Databases → Add database**
- Name: `hardtechhub_catalog_db`

### 5.2 Crear las 6 tablas manualmente

**Glue → Databases → hardtechhub_catalog_db → Tables → Add table → Add table manually**

Para cada tabla, usar **"Edit schema as JSON"** con el esquema correspondiente:

#### Tabla `brands`
- Data source: `s3://<bucket>/db-extracts/catalog/brands/`
- Data format: CSV
```json
[
  {"Name": "id", "Type": "int"},
  {"Name": "name", "Type": "string"},
  {"Name": "country", "Type": "string"}
]
```

#### Tabla `categories`
- Data source: `s3://<bucket>/db-extracts/catalog/categories/`
- Data format: CSV
```json
[
  {"Name": "id", "Type": "int"},
  {"Name": "name", "Type": "string"},
  {"Name": "description", "Type": "string"}
]
```

#### Tabla `products`
- Data source: `s3://<bucket>/db-extracts/catalog/products/`
- Data format: CSV
```json
[
  {"Name": "id", "Type": "int"},
  {"Name": "category_id", "Type": "int"},
  {"Name": "brand_id", "Type": "int"},
  {"Name": "sku", "Type": "string"},
  {"Name": "name", "Type": "string"},
  {"Name": "description", "Type": "string"},
  {"Name": "price", "Type": "double"},
  {"Name": "specs", "Type": "string"},
  {"Name": "image_url", "Type": "string"},
  {"Name": "is_active", "Type": "boolean"},
  {"Name": "created_at", "Type": "string"}
]
```
⚠️ **Importante**: esta tabla tiene una columna (`specs`) con JSON dentro de un campo CSV, con comas internas. Después de crearla, hay que editarla:
- **Edit table → Advanced properties → Serde information**
- Serialization lib: `org.apache.hadoop.hive.serde2.OpenCSVSerde`
- Serde parameters:
  - `separatorChar` = `,`
  - `quoteChar` = `"`

#### Tabla `users`
- Data source: `s3://<bucket>/db-extracts/identity/users/`
- Data format: JSON
```json
[
  {"Name": "user_id", "Type": "string"},
  {"Name": "email", "Type": "string"},
  {"Name": "password_hash", "Type": "string"},
  {"Name": "roles", "Type": "array<string>"},
  {"Name": "preferences", "Type": "string"},
  {"Name": "created_at", "Type": "string"}
]
```
Nota: el archivo generado por `ingest-identity` está en formato **JSON Lines** (un objeto JSON por línea, sin array envolvente) — es el formato que Athena necesita para leer JSON correctamente.

#### Tabla `orders`
- Data source: `s3://<bucket>/db-extracts/order/orders/`
- Data format: CSV
```json
[
  {"Name": "id", "Type": "int"},
  {"Name": "user_id", "Type": "string"},
  {"Name": "status", "Type": "string"},
  {"Name": "subtotal", "Type": "double"},
  {"Name": "tax", "Type": "double"},
  {"Name": "shipping_cost", "Type": "double"},
  {"Name": "total_amount", "Type": "double"},
  {"Name": "created_at", "Type": "string"},
  {"Name": "updated_at", "Type": "string"}
]
```

#### Tabla `order_items`
- Data source: `s3://<bucket>/db-extracts/order/order_items/`
- Data format: CSV
```json
[
  {"Name": "id", "Type": "int"},
  {"Name": "order_id", "Type": "int"},
  {"Name": "product_id", "Type": "int"},
  {"Name": "product_sku", "Type": "string"},
  {"Name": "product_name", "Type": "string"},
  {"Name": "quantity", "Type": "int"},
  {"Name": "unit_price", "Type": "double"},
  {"Name": "subtotal", "Type": "double"}
]
```

### 5.3 Ignorar la fila de encabezado (headers) en las tablas CSV

Para **todas** las tablas en formato CSV (`brands`, `categories`, `products`, `orders`, `order_items` — NO `users`, que es JSON), agregar esta propiedad:

**Edit table → Table properties → Add**
- Key: `skip.header.line.count`
- Value: `1`

Sin esto, Athena intenta leer la fila de encabezado (`id,name,...`) como si fuera un registro real y falla con `NumberFormatException`.

---

## 6. Cómo recrear las consultas y vistas de Athena

### 6.1 Configurar el bucket de resultados

**Athena → Settings → Query result location**
```
s3://<bucket>/athena-results/
```

### 6.2 Seleccionar la base de datos

En el panel izquierdo de Athena, elegir `hardtechhub_catalog_db`.

### 6.3 Las 4 consultas (JOIN)

**1. Productos con su categoría y marca**
```sql
SELECT
    p.id AS product_id, p.name AS product_name, p.sku, p.price,
    c.name AS category_name, b.name AS brand_name, b.country AS brand_country
FROM products p
JOIN categories c ON p.category_id = c.id
JOIN brands b ON p.brand_id = b.id
ORDER BY p.price DESC;
```

**2. Detalle de órdenes con los productos comprados**
```sql
SELECT
    o.id AS order_id, o.user_id, o.status, o.total_amount,
    oi.product_name, oi.quantity, oi.unit_price, oi.subtotal
FROM orders o
JOIN order_items oi ON o.id = oi.order_id
ORDER BY o.id;
```

**3. Total gastado por usuario**
```sql
SELECT
    u.user_id, u.email,
    COUNT(o.id) AS total_ordenes,
    SUM(o.total_amount) AS total_gastado
FROM users u
JOIN orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.email
ORDER BY total_gastado DESC;
```

**4. Productos más vendidos**
```sql
SELECT
    p.name AS product_name, p.sku, c.name AS category_name,
    SUM(oi.quantity) AS unidades_vendidas,
    SUM(oi.subtotal) AS ingresos_generados
FROM order_items oi
JOIN products p ON oi.product_sku = p.sku
JOIN categories c ON p.category_id = c.id
GROUP BY p.name, p.sku, c.name
ORDER BY unidades_vendidas DESC;
```

### 6.4 Las 2 vistas

```sql
CREATE OR REPLACE VIEW vista_productos_completo AS
SELECT
    p.id AS product_id, p.name AS product_name, p.sku, p.price,
    c.name AS category_name, b.name AS brand_name, b.country AS brand_country
FROM products p
JOIN categories c ON p.category_id = c.id
JOIN brands b ON p.brand_id = b.id;
```

```sql
CREATE OR REPLACE VIEW vista_ventas_por_producto AS
SELECT
    p.name AS product_name, p.sku, c.name AS category_name,
    SUM(oi.quantity) AS unidades_vendidas,
    SUM(oi.subtotal) AS ingresos_generados
FROM order_items oi
JOIN products p ON oi.product_sku = p.sku
JOIN categories c ON p.category_id = c.id
GROUP BY p.name, p.sku, c.name;
```

Probar cada vista con:
```sql
SELECT * FROM vista_productos_completo;
SELECT * FROM vista_ventas_por_producto ORDER BY unidades_vendidas DESC;
```

---
