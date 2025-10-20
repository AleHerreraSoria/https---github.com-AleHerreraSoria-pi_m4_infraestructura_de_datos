## **Avance #2: Ingesta de Datos con Airbyte**

En esta segunda fase, el objetivo fue construir el primer componente funcional de nuestro pipeline: la ingesta de datos. Utilizando  **Airbyte Cloud** , configuramos y ejecutamos las "tuberías" para mover datos desde dos fuentes distintas (una API pública y una base de datos PostgreSQL) hacia la capa `bronze` de nuestro Data Lake en AWS S3.

---

### ### 1. Configuración del Destino en Airbyte (AWS S3)

Antes de poder ingestar datos, fue necesario configurar el destino final. Para ello, se siguieron las mejores prácticas de seguridad en AWS.

**a. Creación de un Usuario IAM Dedicado:**
Para permitir que Airbyte acceda a nuestro bucket de forma segura, se creó un usuario programático en AWS IAM con permisos específicos y limitados (principio de mínimo privilegio).

* **Nombre de Usuario:** `airbyte-s3-writer`
* **Política de Permisos (JSON):** Se creó una política personalizada que solo permite a este usuario listar el bucket y leer/escribir/borrar objetos dentro de él, previniendo cualquier acceso no autorizado a otros recursos de AWS.

**b. Configuración del Destino en Airbyte:**
Con las credenciales del usuario IAM, se configuró el destino en la interfaz de Airbyte Cloud.

* **Nombre del Destino:** `Data Lake S3`
* **Bucket S3:** `weatherlytics-datalake-dev-us-east-2`
* **Ruta del Bucket:** `bronze/` ( crucial para asegurar que los datos crudos aterricen en la capa correcta).
* **Región:** `us-east-2`
* **Formato de Salida:**  **Parquet** , un formato columnar eficiente, ideal para análisis posteriores con Spark.

---

### ### 2. Configuración de las Fuentes de Datos

**a. Fuente #1: API Pública (WeatherAPI)**
Para cumplir con la consigna de conectar a una API pública, se utilizó el conector **File** de Airbyte, configurado para leer desde una URL HTTPS.

* **Nombre de la Fuente:** `API Publica - WeatherAPI`
* **Proveedor:** `HTTPS – Public Web`
* **URL:** Se construyó la URL completa, incluyendo la API Key y la ciudad a consultar, para realizar la petición GET:
  ```
  http://api.weatherapi.com/v1/current.json?key=[TU_API_KEY]&q=Riohacha
  ```
* **Nombre del Dataset:** `weather_api_data` (Este nombre se usará para la carpeta de destino en S3).

**b. Fuente #2: Base de Datos PostgreSQL (Supabase)**
La conexión a una base de datos PostgreSQL presentó un desafío significativo y una valiosa lección de ingeniería de datos.

* **Intentos Fallidos:** Los intentos iniciales de conectar a bases de datos PostgreSQL públicas (`bit.io`, `aiven.io`) fallaron consistentemente debido a un error de  **timeout** . El análisis de los logs de Airbyte reveló que, aunque el proceso de fondo sí lograba conectarse, la interfaz de usuario de Airbyte Cloud tenía un tiempo de espera demasiado corto (~10s) que estas bases de datos públicas y lentas no podían cumplir.
* **La Solución Profesional (Pivote a Supabase):** Para superar este obstáculo, se decidió tomar control total del entorno creando una base de datos PostgreSQL dedicada y gratuita en  **Supabase** . Esto nos proporcionó una base de datos rápida y confiable.
* **Configuración Crítica del Firewall:** El paso clave para el éxito fue configurar las **"Network Restrictions"** (Restricciones de Red) en el panel de Supabase. Se añadieron a la lista blanca (IPv4) todas las direcciones IP de salida proporcionadas por Airbyte Cloud, permitiendo así el tráfico entrante desde sus servidores.
* **Configuración Final de la Fuente en Airbyte:**
  * **Nombre de la Fuente:** `Mi DB - Supabase (con Firewall)`
  * **Host:** Se utilizó el host del *pooler* en modo sesión de Supabase (ej: `aws-0-us-east-1.pooler.supabase.com`), que garantiza compatibilidad de red (IPv4/IPv6).
  * **Puerto:** `5432`
  * **Base de Datos:** `postgres`
  * **Usuario:** `postgres`
  * **Método de Replicación:** `Detect Changes with Xmin System Column`, como se recomienda para conexiones de solo lectura sin permisos de CDC.
  * **Modo SSL:** `require`, ya que Supabase exige conexiones seguras.

---

### ### 3. Creación y Ejecución de las Conexiones

Con las fuentes y el destino configurados, se procedió a crear las dos conexiones, configurando ambas con una frecuencia de replicación **Manual** para tener control total durante el desarrollo.

1. **`API WeatherAPI to S3`** : Conectó la fuente de la API con el destino S3.
2. **`DB Supabase to S3`** : Conectó la fuente de la base de datos PostgreSQL con el destino S3.

Ambas conexiones se ejecutaron manualmente y se completaron con éxito, como se evidencia en el historial de sincronización de Airbyte.

---

### ### 4. Validación Final

El último paso fue validar que los datos se hubieran ingestado correctamente en nuestro Data Lake, cumpliendo con la estructura y formato definidos.

* Se accedió al bucket `weatherlytics-datalake-dev-us-east-2` a través de la consola de AWS.
* Dentro de la carpeta `bronze/`, se verificó la existencia de las siguientes estructuras de carpetas, creadas automáticamente por Airbyte:
  * **`public/public/actor/`** : Conteniendo los datos de la tabla `actor` de la base de datos PostgreSQL.
  * **`weather_api_data/`** : Conteniendo los datos extraídos de la WeatherAPI.
* Se navegó dentro de estas carpetas para confirmar que los archivos de datos fueron escritos correctamente en formato  **`.parquet`** .

Con esta validación, se dio por finalizado y completado con éxito el `Avance #2` del proyecto.

---

### ### Recursos Utilizados

* **Airbyte Cloud (Free Tier):** Plataforma central para la ingesta de datos.
* **AWS S3:** Para el almacenamiento en la capa `bronze` de nuestro Data Lake.
* **AWS IAM:** Para la gestión segura de credenciales de acceso.
* **Supabase (Free Tier):** Para provisionar una base de datos PostgreSQL rápida, confiable y con control de firewall.
* **WeatherAPI.com:** Como fuente de datos de API pública.
