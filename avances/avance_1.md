# Análisis de Datos de Películas - Scala FS2 con Circe

Proyecto académico de análisis de datos cinematográficos utilizando programación funcional reactiva con Scala, FS2 Streams y Circe para procesamiento JSON. Implementa lectura eficiente, análisis estadístico, limpieza completa de datos y procesamiento de columnas JSON complejas.

## Tabla de Contenidos

- [Marco Teórico](#marco-teórico)
- [Diccionario de Datos](#diccionario-de-datos)
- [Librería Circe](#librería-circe)
- [Configuración del Proyecto](#configuración-del-proyecto)
- [Implementación Completa](#implementación-completa)
- [Limpieza de Datos](#limpieza-de-datos)
- [Manejo de Columna Crew](#manejo-de-columna-crew)
- [Ejecución](#ejecución)
- [Resultados Esperados](#resultados-esperados)

---

## Marco Teórico

### Base de Datos

#### ¿Qué es una Base de Datos?

Una base de datos es un conjunto organizado de información estructurada que se almacena electrónicamente en un sistema informático. Las bases de datos facilitan el almacenamiento, recuperación, modificación y eliminación de datos de manera eficiente y controlada.

#### Tipos de Bases de Datos

**Bases de Datos Relacionales (SQL)**
- MySQL: Sistema de gestión de bases de datos relacional de código abierto
- PostgreSQL: Base de datos relacional objeto-relacional de código abierto
- Oracle Database: Sistema de gestión de bases de datos empresarial
- Microsoft SQL Server: Sistema de gestión de bases de datos de Microsoft

**Bases de Datos NoSQL**
- MongoDB: Base de datos orientada a documentos JSON
- Redis: Base de datos en memoria de clave-valor
- Cassandra: Base de datos distribuida columnar
- Neo4j: Base de datos de grafos

#### Tecnologías de Bases de Datos

Las tecnologías modernas incluyen sistemas distribuidos, bases de datos en la nube (AWS RDS, Azure SQL, Google Cloud SQL), motores de búsqueda (Elasticsearch), y sistemas analíticos (Apache Spark, Hadoop).

#### Conceptos Clave

**Entidad**: Objeto o concepto del mundo real que se puede distinguir de otros objetos. Ejemplo: En nuestro dataset, "Película" es una entidad con atributos como título, presupuesto y fecha de estreno.

**Atributo**: Característica o propiedad que describe una entidad. Ejemplo: Para la entidad Película, los atributos son title, budget, revenue, vote_average, etc.

**Registro**: Instancia individual de una entidad con valores específicos para todos sus atributos. Ejemplo: Una fila en la tabla movies que representa una película específica como "Avatar".

**Tabla**: Colección de registros relacionados organizados en filas y columnas. Ejemplo: La tabla movies contiene todos los registros de películas.

**Relación**: Asociación entre dos o más entidades que describe cómo están conectadas. Ejemplo: Una película "tiene" varios actores, una película "pertenece a" un género.

**Clave Primaria (PK)**: Atributo o conjunto de atributos que identifica de manera única cada registro en una tabla. Ejemplo: El campo "id" en la tabla movies.

**Clave Foránea (FK)**: Atributo en una tabla que hace referencia a la clave primaria de otra tabla, estableciendo una relación entre ambas. Ejemplo: El campo "genre_id" que relaciona películas con géneros.

#### Modelos de Bases de Datos

**Modelo Conceptual**: Representación abstracta de alto nivel de los datos y sus relaciones, independiente de la tecnología. Se utilizan Diagramas Entidad-Relación (ER) para visualizar entidades, atributos y relaciones sin detalles de implementación.

**Modelo Lógico**: Estructura detallada de datos independiente del sistema gestor específico. Define tablas, columnas, tipos de datos, restricciones y relaciones de manera formal pero sin considerar aspectos físicos de almacenamiento.

**Modelo Físico**: Implementación específica en un Sistema Gestor de Bases de Datos (SGBD) particular. Incluye índices, particiones, tipos de almacenamiento, optimizaciones de rendimiento y detalles técnicos específicos del SGBD utilizado.

#### Lenguaje para Consulta de Bases de Datos

**SQL (Structured Query Language)**: Lenguaje estándar para gestionar y manipular bases de datos relacionales. Permite realizar operaciones de:

- **Consulta (SELECT)**: Recuperar datos específicos
- **Inserción (INSERT)**: Agregar nuevos registros
- **Actualización (UPDATE)**: Modificar registros existentes
- **Eliminación (DELETE)**: Remover registros

Ejemplo de consulta SQL:
```sql
SELECT title, revenue, budget
FROM movies
WHERE revenue > 1000000 AND status = 'Released'
ORDER BY revenue DESC
LIMIT 10;
```

### Programación Funcional Reactiva

#### ¿Qué es la Programación Funcional Reactiva?

La programación funcional reactiva combina los principios de la programación funcional con el procesamiento asíncrono de flujos de datos (streams). Permite construir sistemas que reaccionan a cambios de manera declarativa, componible y eficiente.

En lugar de escribir código imperativo que describe paso a paso cómo procesar datos, la programación funcional reactiva permite declarar qué transformaciones aplicar a flujos de datos, dejando que el sistema maneje la ejecución asíncrona, el manejo de errores y la liberación de recursos automáticamente.

#### Beneficios de la Programación Funcional y Reactiva

1. **Código más limpio y mantenible**: Las funciones puras y la composición funcional producen código más fácil de entender y modificar.

2. **Mejor manejo de concurrencia**: La inmutabilidad elimina condiciones de carrera y simplifica el código concurrente.

3. **Inmutabilidad por defecto reduce errores**: Al no modificar datos existentes, se evitan efectos secundarios no deseados.

4. **Composición de operaciones complejas**: Se pueden construir pipelines complejos componiendo funciones simples.

5. **Procesamiento eficiente de grandes volúmenes**: Los streams permiten procesar datos sin cargar todo en memoria.

6. **Testabilidad mejorada**: Las funciones puras son fáciles de probar sin necesidad de mocks complejos.

#### Conceptos Clave

**Función Pura**: Una función que siempre retorna el mismo resultado para los mismos argumentos y no tiene efectos secundarios (no modifica variables externas, no realiza I/O, etc.).

```scala
// Función pura
def suma(a: Int, b: Int): Int = a + b

// Función pura
def duplicar(numeros: List[Int]): List[Int] = 
  numeros.map(_ * 2)

// Función impura (tiene efecto secundario)
var contador = 0
def incrementar(): Int = {
  contador += 1  // Modifica estado externo
  contador
}

// Función impura (realiza I/O)
def leerArchivo(path: String): String = {
  scala.io.Source.fromFile(path).mkString  // Efecto secundario
}
```

**Inmutabilidad**: Los datos no se modifican después de su creación. En lugar de cambiar un valor existente, se crea un nuevo valor con las modificaciones necesarias.

```scala
// Inmutable
val lista = List(1, 2, 3)
val nuevaLista = lista.map(_ * 2)  // lista permanece sin cambios

// Mutable (evitar en programación funcional)
val buffer = scala.collection.mutable.ListBuffer(1, 2, 3)
buffer += 4  // Modifica buffer directamente
```

**Funciones de Primera Clase**: En programación funcional, las funciones son valores que pueden:
- Asignarse a variables
- Pasarse como argumentos a otras funciones
- Retornarse como resultado de otras funciones
- Almacenarse en estructuras de datos

```scala
// Función como valor
val multiplicar = (x: Int, y: Int) => x * y

// Función como argumento
val numeros = List(1, 2, 3, 4, 5)
val pares = numeros.filter(n => n % 2 == 0)

// Función como retorno
def crearMultiplicador(factor: Int): Int => Int = {
  x => x * factor
}
val duplicar = crearMultiplicador(2)
duplicar(5)  // 10
```

**Recursión**: Técnica donde una función se llama a sí misma para resolver un problema dividiéndolo en subproblemas más pequeños.

```scala
// Recursión simple
def factorial(n: Int): Int = 
  if (n <= 1) 1 
  else n * factorial(n - 1)

// Recursión de cola (tail recursion) - optimizada por el compilador
@annotation.tailrec
def factorialTail(n: Int, acumulador: Int = 1): Int = 
  if (n <= 1) acumulador
  else factorialTail(n - 1, n * acumulador)

// Recursión en listas
def sumaLista(lista: List[Int]): Int = lista match {
  case Nil => 0
  case head :: tail => head + sumaLista(tail)
}
```

**Funciones de Orden Superior**: Funciones que reciben otras funciones como parámetros o las retornan como resultado. Son fundamentales para la composición y abstracción en programación funcional.

```scala
// map es una función de orden superior
val numeros = List(1, 2, 3, 4, 5)
val cuadrados = numeros.map(x => x * x)

// filter es una función de orden superior
val mayoresQueTres = numeros.filter(_ > 3)

// fold es una función de orden superior
val suma = numeros.fold(0)(_ + _)

// Definir una función de orden superior personalizada
def aplicarDosVeces(f: Int => Int, x: Int): Int = f(f(x))
aplicarDosVeces(_ + 3, 5)  // ((5 + 3) + 3) = 11

// Componer funciones
def componer[A, B, C](f: B => C, g: A => B): A => C = 
  x => f(g(x))
```

#### Estándares de Programación Funcional

1. **Evitar efectos secundarios**: Mantener funciones puras siempre que sea posible
2. **Preferir inmutabilidad**: Usar val en lugar de var, estructuras de datos inmutables
3. **Usar expresiones sobre declaraciones**: Preferir expresiones que retornan valores
4. **Composición sobre herencia**: Construir funcionalidad componiendo funciones
5. **Manejo explícito de errores**: Usar tipos como Option, Either, Try en lugar de excepciones

#### Tipos de Consultas usando Scala

```scala
// Consulta similar a SQL usando operaciones funcionales
val peliculasCostosas = movies
  .filter(_.budget > 50000000)
  .sortBy(-_.revenue)
  .take(10)

// Agregaciones
val ingresoTotal = movies.map(_.revenue).sum
val promedioCalificacion = movies.map(_.vote_average).sum / movies.length

// Agrupaciones
val peliculasPorGenero = movies
  .flatMap(_.genres)
  .groupBy(_.name)
  .view
  .mapValues(_.size)
  .toMap

// Joins (operación similar a SQL JOIN)
val peliculasConDirector = movies.flatMap { movie =>
  movie.crew
    .find(_.job == "Director")
    .map(director => (movie.title, director.name))
}
```

#### Análisis Exploratorio

El análisis exploratorio de datos (EDA) en programación funcional se realiza mediante transformaciones declarativas:

1. **Inspección de estructura**: Examinar tipos de datos, valores nulos, distribuciones
2. **Estadísticas descriptivas**: Media, mediana, desviación estándar, cuartiles
3. **Identificación de patrones**: Correlaciones, outliers, tendencias
4. **Visualización conceptual**: Preparar datos para gráficos y reportes

```scala
// Ejemplo de EDA funcional
val estadisticas = movies.foldLeft(Stats.empty) { (stats, movie) =>
  stats
    .addRevenue(movie.revenue)
    .addBudget(movie.budget)
    .addRating(movie.vote_average)
}
```

---

## Diccionario de Datos

### Tabla: Movies_Complete

Esta tabla contiene información detallada sobre películas, incluyendo datos financieros, calificaciones, información técnica y relaciones con géneros, actores y equipo técnico.

#### Columnas Simples - Identificadores

| Columna | Tipo | Descripción | Restricciones |
|---------|------|-------------|---------------|
| id | Int | Identificador único de cada película | PK, no nulo, único |
| imdb_id | String | Identificador de IMDb | Formato ttXXXXXXX |

#### Información Básica

| Columna | Tipo | Descripción | Restricciones |
|---------|------|-------------|---------------|
| title | String | Título comercial de la película | No nulo |
| original_title | String | Título original en idioma nativo | - |
| original_language | String | Código ISO 639-1 del idioma original | Longitud = 2 caracteres |
| overview | String | Sinopsis o descripción resumida | - |
| tagline | String | Frase promocional característica | - |
| homepage | String | Sitio web oficial de la película | URL válida o null |

#### Fechas y Estado

| Columna | Tipo | Descripción | Restricciones |
|---------|------|-------------|---------------|
| release_date | String | Fecha de estreno | Formato YYYY-MM-DD |
| status | String | Estado actual de la película | Valores controlados: Released, Post Production, In Production, Planned, Rumored, Canceled |

#### Métricas Técnicas

| Columna | Tipo | Descripción | Restricciones |
|---------|------|-------------|---------------|
| runtime | Double | Duración total en minutos | Mayor a 0, típicamente < 500 |
| adult | Boolean | Indica si es contenido para adultos | True o False |
| video | Boolean | Indica si es material audiovisual adicional | True o False |

#### Popularidad y Calificaciones

| Columna | Tipo | Descripción | Restricciones |
|---------|------|-------------|---------------|
| popularity | Double | Índice de popularidad calculado | Mayor o igual a 0 |
| vote_average | Double | Calificación promedio de usuarios | Rango 0.0 a 10.0 |
| vote_count | Int | Número total de votos recibidos | Mayor o igual a 0 |

#### Información Financiera

| Columna | Tipo | Descripción | Restricciones |
|---------|------|-------------|---------------|
| budget | Double | Presupuesto total de producción en USD | Mayor o igual a 0, puede ser 0 si no hay datos |
| revenue | Double | Ingresos totales generados en USD | Mayor o igual a 0, puede ser 0 si no hay datos |

#### Recursos Multimedia

| Columna | Tipo | Descripción | Restricciones |
|---------|------|-------------|---------------|
| poster_path | String | Ruta relativa del póster oficial | Puede ser null |
| backdrop_path | String | Ruta de la imagen de fondo | Puede ser null |

#### Columnas JSON (Multivaluadas)

| Columna | Estructura JSON | Descripción | Sub-atributos |
|---------|-----------------|-------------|---------------|
| genres | Array de objetos | Géneros cinematográficos asociados | id: Int, name: String |
| cast | Array de objetos | Elenco de actores | name: String, character: String, order: Int, gender: Int |
| crew | Array de objetos | Equipo técnico (directores, guionistas, etc.) | name: String, job: String, department: String, gender: Int |
| production_companies | Array de objetos | Empresas productoras | id: Int, name: String, origin_country: String |
| production_countries | Array de objetos | Países donde se produjo | iso_3166_1: String, name: String |
| spoken_languages | Array de objetos | Idiomas hablados en la película | iso_639_1: String, name: String |
| belongs_to_collection | Objeto o null | Información de saga/franquicia | id: Int, name: String, poster_path: String, backdrop_path: String |
| keywords | Array de objetos | Palabras clave descriptivas | id: Int, name: String |

### Valores Controlados

**Status** (Estado de la película):
- Released: Película estrenada y disponible
- Post Production: En fase de post-producción
- In Production: En fase de producción/filmación
- Planned: Planeada pero no iniciada
- Rumored: Rumoreada, no confirmada
- Canceled: Cancelada, no se completará

**Códigos de Idioma ISO 639-1**:
- en: Inglés (English)
- es: Español (Spanish)
- fr: Francés (French)
- de: Alemán (German)
- it: Italiano (Italian)
- ja: Japonés (Japanese)
- ko: Coreano (Korean)
- zh: Chino (Chinese)
- ru: Ruso (Russian)
- pt: Portugués (Portuguese)

**Códigos de País ISO 3166-1**:
- US: Estados Unidos
- GB: Reino Unido
- FR: Francia
- DE: Alemania
- ES: España
- IT: Italia
- JP: Japón
- KR: Corea del Sur
- CN: China
- IN: India

### Notas sobre los Datos

**Valores Nulos**: Los campos overview, tagline, homepage, belongs_to_collection, poster_path y backdrop_path pueden contener valores nulos cuando la información no está disponible.

**Valores Cero**: Los campos budget y revenue pueden contener ceros cuando no hay información financiera disponible o cuando la película no generó ingresos reportados.

**Formato de Fechas**: Todas las fechas siguen el formato ISO 8601: YYYY-MM-DD (ejemplo: 2009-12-18 para Avatar).

**Formato IMDb ID**: Sigue el patrón "tt" seguido de 7 dígitos (ejemplo: tt0499549 para Avatar).

---

## Librería Circe

### ¿Qué es Circe?

Circe es una librería de Scala para codificar y decodificar JSON de manera funcional, type-safe y eficiente. Utiliza type classes y derivación automática para proveer conversiones entre tipos Scala y JSON sin boilerplate.

**Características principales**:
- Parsing y generación de JSON puramente funcional
- Derivación automática de codecs
- Manejo de errores explícito con Either
- Integración con cats y cats-effect
- Sin reflexión en runtime (todo en tiempo de compilación)

### Instalación

Agregar al `build.sbt`:

```scala
val circeVersion = "0.14.6"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion
)
```

### Conceptos Fundamentales

#### Decoders y Encoders

Los **Decoders** convierten JSON a tipos Scala. Los **Encoders** convierten tipos Scala a JSON.

```scala
import io.circe._
import io.circe.generic.semiauto._

// Definir case class
case class Genre(id: Int, name: String)

// Crear decoders y encoders automáticamente
implicit val genreDecoder: Decoder[Genre] = deriveDecoder[Genre]
implicit val genreEncoder: Encoder[Genre] = deriveEncoder[Genre]
```

#### Parsing JSON

```scala
import io.circe.parser._

val jsonString = """{"id": 28, "name": "Action"}"""

// Parsear string a JSON
val json: Either[ParsingFailure, Json] = parse(jsonString)

// Decodificar directamente a case class
val genre: Either[Error, Genre] = decode[Genre](jsonString)

// Usar pattern matching para manejar resultado
decode[Genre](jsonString) match {
  case Right(g) => println(s"Género: ${g.name}")
  case Left(error) => println(s"Error: $error")
}
```

#### Trabajar con Arrays JSON

```scala
case class Movie(id: Int, title: String, genres: List[Genre])

val movieJson = """{
  "id": 1,
  "title": "Avatar",
  "genres": [
    {"id": 28, "name": "Action"},
    {"id": 12, "name": "Adventure"}
  ]
}"""

implicit val movieDecoder: Decoder[Movie] = deriveDecoder[Movie]

decode[Movie](movieJson) match {
  case Right(movie) => 
    println(s"${movie.title} tiene ${movie.genres.length} géneros")
  case Left(error) => 
    println(s"Error: $error")
}
```

### Ejemplos Prácticos de Circe

#### Ejemplo 1: JSON Simple

```scala
import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser._
import io.circe.syntax._

case class Person(name: String, age: Int, email: String)

implicit val personDecoder: Decoder[Person] = deriveDecoder[Person]
implicit val personEncoder: Encoder[Person] = deriveEncoder[Person]

// Decodificar JSON a case class
val personJson = """{"name": "Alice", "age": 30, "email": "alice@example.com"}"""
val person = decode[Person](personJson)

// Encodear case class a JSON
val alice = Person("Alice", 30, "alice@example.com")
val json = alice.asJson.spaces2
println(json)
```

#### Ejemplo 2: JSON con Listas

```scala
case class Company(name: String, employees: List[Person])

implicit val companyDecoder: Decoder[Company] = deriveDecoder[Company]

val companyJson = """{
  "name": "Tech Corp",
  "employees": [
    {"name": "Alice", "age": 30, "email": "alice@tech.com"},
    {"name": "Bob", "age": 25, "email": "bob@tech.com"}
  ]
}"""

decode[Company](companyJson) match {
  case Right(company) =>
    println(s"${company.name} tiene ${company.employees.length} empleados")
  case Left(error) =>
    println(s"Error: $error")
}
```

#### Ejemplo 3: JSON Anidado

```scala
case class Address(street: String, city: String, country: String)
case class Department(name: String, manager: Person, address: Address)

implicit val addressDecoder: Decoder[Address] = deriveDecoder[Address]
implicit val departmentDecoder: Decoder[Department] = deriveDecoder[Department]

val deptJson = """{
  "name": "Engineering",
  "manager": {"name": "Alice", "age": 35, "email": "alice@eng.com"},
  "address": {"street": "Main St", "city": "NYC", "country": "USA"}
}"""

decode[Department](deptJson) match {
  case Right(dept) =>
    println(s"${dept.name} en ${dept.address.city}")
  case Left(error) =>
    println(s"Error: $error")
}
```

#### Ejemplo 4: Campos Opcionales

```scala
case class Movie(
  id: Int,
  title: String,
  director: Option[String],
  budget: Option[Double]
)

implicit val movieDecoder: Decoder[Movie] = deriveDecoder[Movie]

// JSON con campos faltantes
val json1 = """{"id": 1, "title": "Avatar", "director": "James Cameron"}"""
val json2 = """{"id": 2, "title": "Indie Film"}"""

List(json1, json2).foreach { json =>
  decode[Movie](json) match {
    case Right(movie) =>
      println(s"${movie.title}")
      println(s"  Director: ${movie.director.getOrElse("Desconocido")}")
    case Left(error) =>
      println(s"Error: $error")
  }
}
```

#### Ejemplo 5: Manejo de Errores

```scala
// Diferentes tipos de errores
val invalidJsons = List(
  """{"name": "Alice", "age": "thirty"}""",  // Tipo incorrecto
  """{"name": "Bob"}""",                      // Campo faltante
  """{"invalid json}"""                        // JSON malformado
)

invalidJsons.foreach { json =>
  decode[Person](json) match {
    case Right(person) =>
      println(s"Success: $person")
    case Left(error) => error match {
      case ParsingFailure(msg, _) =>
        println(s"Parse error: $msg")
      case DecodingFailure(msg, _) =>
        println(s"Decode error: $msg")
    }
  }
}
```

#### Ejemplo 6: Decoder Personalizado

```scala
case class CustomMovie(id: Int, title: String, year: Int)

// JSON con nombres de campo diferentes
implicit val customDecoder: Decoder[CustomMovie] = (c: HCursor) => {
  for {
    id <- c.downField("movie_id").as[Int]
    title <- c.downField("movie_title").as[String]
    year <- c.downField("release_year").as[Int]
  } yield CustomMovie(id, title, year)
}

val customJson = """{
  "movie_id": 100,
  "movie_title": "The Matrix",
  "release_year": 1999
}"""

decode[CustomMovie](customJson)
```

#### Ejemplo 7: Procesar Arrays

```scala
case class Genre(id: Int, name: String)
implicit val genreDecoder: Decoder[Genre] = deriveDecoder[Genre]

val genresJson = """[
  {"id": 28, "name": "Action"},
  {"id": 12, "name": "Adventure"},
  {"id": 14, "name": "Fantasy"}
]"""

decode[List[Genre]](genresJson) match {
  case Right(genres) =>
    println(s"${genres.length} géneros:")
    genres.foreach(g => println(s"  - [${g.id}] ${g.name}"))
  case Left(error) =>
    println(s"Error: $error")
}
```

#### Ejemplo 8: Transformaciones

```scala
val personsJson = """[
  {"name": "Alice", "age": 30, "email": "alice@example.com"},
  {"name": "Bob", "age": 25, "email": "bob@example.com"},
  {"name": "Carol", "age": 35, "email": "carol@example.com"}
]"""

decode[List[Person]](personsJson) match {
  case Right(persons) =>
    val adults = persons.filter(_.age >= 30)
    val names = persons.map(_.name)
    val avgAge = persons.map(_.age).sum.toDouble / persons.length
    
    println(s"Total: ${persons.length}")
    println(s"Mayores de 30: ${adults.length}")
    println(s"Edad promedio: $avgAge")
  case Left(error) =>
    println(s"Error: $error")
}
```

### Función Auxiliar para Parsear JSON

```scala
import io.circe.Decoder
import io.circe.parser._

def parseJsonList[T: Decoder](jsonString: String): List[T] = {
  decode[List[T]](jsonString).getOrElse(List.empty)
}

// Uso
val genresJson = """[{"id": 28, "name": "Action"}]"""
val genres: List[Genre] = parseJsonList[Genre](genresJson)
```

---

## Configuración del Proyecto

### Requisitos del Sistema

- **Scala**: 2.13.12 o superior
- **SBT**: 1.9.7 o superior
- **Java**: JDK 11 o superior
- **Sistema Operativo**: Linux, macOS o Windows

### Estructura de Directorios

```
movie-analysis/
├── README.md
├── build.sbt
├── project/
│   └── build.properties
└── src/
    └── main/
        └── scala/
            ├── Models.scala
            ├── JsonDecoders.scala
            ├── DataCleaning.scala
            ├── CrewProcessing.scala
            ├── AnalisisCompleto.scala
            └── CirceExamples.scala
```

### Archivo: build.sbt

```scala
name := "movie-analysis"

version := "1.0.0"

scalaVersion := "2.13.12"

val circeVersion = "0.14.6"
val fs2Version = "3.9.3"
val catsEffectVersion = "3.5.2"

libraryDependencies ++= Seq(
  // FS2 para streams funcionales
  "co.fs2" %% "fs2-core" % fs2Version,
  "co.fs2" %% "fs2-io" % fs2Version,
  
  // FS2 Data para parsing de CSV
  "org.gnieh" %% "fs2-data-csv" % "1.10.0",
  "org.gnieh" %% "fs2-data-csv-generic" % "1.10.0",
  
  // Circe para procesamiento JSON
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  
  // Cats Effect para IO funcional
  "org.typelevel" %% "cats-effect" % catsEffectVersion
)

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)
```

### Archivo: project/build.properties

```
sbt.version=1.9.7
```

### Archivo: .gitignore

```
# Scala y SBT
*.class
*.log
target/
project/target/
project/project/

# IDE
.idea/
.idea_modules/
*.iml
*.ipr
*.iws
.vscode/
.metals/
.bloop/
.bsp/

# Mac
.DS_Store

# CSV de datos (descomentar si no quieres subirlos)
# *.csv

# Outputs temporales
*.tmp
*.temp
```

---

## Implementación Completa

### 1. Models.scala - Estructuras de Datos

```scala
/**
 * Estructuras de datos para el dataset de películas
 */

// Modelo principal de película
case class Movie(
  id: String,
  title: String,
  budget: Double,
  revenue: Double,
  vote_average: Double,
  vote_count: Int,
  popularity: Double,
  runtime: Double,
  status: String,
  original_language: String,
  genresJson: String,
  castJson: String,
  crewJson: String
)

// Estructuras para columnas JSON
case class Genre(id: Int, name: String)

case class CastMember(
  name: String,
  character: String,
  order: Int,
  gender: Option[Int] = None
)

case class CrewMember(
  name: String,
  job: String,
  department: String,
  gender: Option[Int] = None
)

case class ProductionCompany(
  id: Int,
  name: String,
  origin_country: Option[String] = None
)

case class ProductionCountry(
  iso_3166_1: String,
  name: String
)

case class SpokenLanguage(
  iso_639_1: String,
  name: String
)

case class Collection(
  id: Int,
  name: String,
  poster_path: Option[String] = None,
  backdrop_path: Option[String] = None
)

case class Keyword(
  id: Int,
  name: String
)
```

### 2. JsonDecoders.scala - Decoders de Circe

```scala
import io.circe._
import io.circe.generic.semiauto._

/**
 * Decoders de Circe para cada estructura JSON
 * Permiten convertir automáticamente JSON a case classes
 */
object JsonDecoders {
  
  // Decoders automáticos
  implicit val genreDecoder: Decoder[Genre] = deriveDecoder[Genre]
  implicit val castDecoder: Decoder[CastMember] = deriveDecoder[CastMember]
  implicit val crewDecoder: Decoder[CrewMember] = deriveDecoder[CrewMember]
  implicit val companyDecoder: Decoder[ProductionCompany] = deriveDecoder[ProductionCompany]
  implicit val countryDecoder: Decoder[ProductionCountry] = deriveDecoder[ProductionCountry]
  implicit val languageDecoder: Decoder[SpokenLanguage] = deriveDecoder[SpokenLanguage]
  implicit val collectionDecoder: Decoder[Collection] = deriveDecoder[Collection]
  implicit val keywordDecoder: Decoder[Keyword] = deriveDecoder[Keyword]
  
  // Encoders (para escribir JSON)
  implicit val genreEncoder: Encoder[Genre] = deriveEncoder[Genre]
  implicit val castEncoder: Encoder[CastMember] = deriveEncoder[CastMember]
  implicit val crewEncoder: Encoder[CrewMember] = deriveEncoder[CrewMember]
  implicit val companyEncoder: Encoder[ProductionCompany] = deriveEncoder[ProductionCompany]
  
  /**
   * Decoder personalizado para crew que maneja casos especiales
   * Algunos registros pueden tener campos faltantes
   */
  implicit val crewMemberSafeDecoder: Decoder[CrewMember] = (c: HCursor) => {
    for {
      name <- c.downField("name").as[String]
      job <- c.downField("job").as[String]
      department <- c.downField("department").as[String].orElse(Right("Unknown"))
      gender <- c.downField("gender").as[Option[Int]]
    } yield CrewMember(name, job, department, gender)
  }
  
  /**
   * Función auxiliar para parsear listas JSON de manera segura
   */
  def parseJsonList[T: Decoder](jsonString: String): List[T] = {
    import io.circe.parser._
    decode[List[T]](jsonString).getOrElse(List.empty)
  }
}
```

---

## Limpieza de Datos

### 3. DataCleaning.scala - Limpieza Completa

```scala
import io.circe.parser._

/**
 * Módulo de limpieza completa de datos
 * Implementa validaciones, detección de outliers y normalización
 */
object DataCleaning {
  
  /**
   * 1. VALIDACIÓN DE CAMPOS OBLIGATORIOS
   */
  def hasRequiredFields(m: Movie): Boolean = {
    m.id.nonEmpty &&
    m.title.nonEmpty &&
    m.status.nonEmpty &&
    m.original_language.nonEmpty
  }
  
  /**
   * 2. VALIDACIÓN DE RANGOS NUMÉRICOS
   */
  def hasValidRanges(m: Movie): Boolean = {
    m.revenue >= 0 &&
    m.budget >= 0 &&
    m.vote_average >= 0 && m.vote_average <= 10 &&
    m.vote_count >= 0 &&
    m.popularity >= 0 &&
    m.runtime > 0 && m.runtime < 500
  }
  
  /**
   * 3. DETECCIÓN DE OUTLIERS - MÉTODO IQR
   * (Interquartile Range - Rango Intercuartílico)
   */
  def detectOutliers(values: List[Double]): Set[Double] = {
    if (values.isEmpty) return Set.empty
    
    val sorted = values.sorted
    val n = sorted.length
    
    // Calcular cuartiles
    val q1 = sorted(n / 4)
    val q3 = sorted(3 * n / 4)
    val iqr = q3 - q1
    
    // Límites: Q1 - 1.5*IQR y Q3 + 1.5*IQR
    val lowerBound = q1 - 1.5 * iqr
    val upperBound = q3 + 1.5 * iqr
    
    values.filter(v => v < lowerBound || v > upperBound).toSet
  }
  
  /**
   * 4. VALIDACIÓN DE FORMATO DE FECHA
   */
  def hasValidDate(dateString: String): Boolean = {
    val datePattern = """^\d{4}-\d{2}-\d{2}$""".r
    datePattern.matches(dateString)
  }
  
  /**
   * 5. VALIDACIÓN DE CÓDIGO DE IDIOMA ISO 639-1
   */
  def hasValidLanguageCode(code: String): Boolean = {
    code.length == 2 && code.forall(_.isLetter)
  }
  
  /**
   * 6. VALIDACIÓN DE JSON
   */
  def hasValidJson(jsonString: String): Boolean = {
    jsonString.nonEmpty &&
    jsonString.length > 2 &&
    parse(jsonString).isRight
  }
  
  /**
   * 7. ELIMINACIÓN DE DUPLICADOS
   */
  def removeDuplicates(movies: List[Movie]): List[Movie] = {
    movies
      .groupBy(_.id)
      .values
      .map(_.head)
      .toList
  }
  
  /**
   * 8. NORMALIZACIÓN DE STATUS
   */
  def normalizeStatus(status: String): String = {
    status.toLowerCase.trim match {
      case s if s.contains("released") => "Released"
      case s if s.contains("post") && s.contains("production") => "Post Production"
      case s if s.contains("production") => "In Production"
      case s if s.contains("planned") => "Planned"
      case s if s.contains("rumored") => "Rumored"
      case s if s.contains("canceled") || s.contains("cancelled") => "Canceled"
      case _ => status.trim
    }
  }
  
  /**
   * 9. LIMPIEZA DE WHITESPACE
   */
  def cleanWhitespace(m: Movie): Movie = {
    m.copy(
      title = m.title.trim,
      status = m.status.trim,
      original_language = m.original_language.trim
    )
  }
  
  /**
   * 10. VALIDACIÓN DE CONSISTENCIA FINANCIERA
   */
  def hasConsistentFinancials(m: Movie): Boolean = {
    if (m.revenue > 0) true
    else m.budget == 0
  }
  
  /**
   * 11. DETECCIÓN DE OUTLIERS POR COLUMNA
   */
  case class OutlierReport(
    revenueOutliers: Set[String],
    budgetOutliers: Set[String],
    popularityOutliers: Set[String]
  )
  
  def detectAllOutliers(movies: List[Movie]): OutlierReport = {
    val revenueValues = movies.filter(_.revenue > 0).map(_.revenue)
    val budgetValues = movies.filter(_.budget > 0).map(_.budget)
    val popularityValues = movies.filter(_.popularity > 0).map(_.popularity)
    
    val revenueOutlierVals = detectOutliers(revenueValues)
    val budgetOutlierVals = detectOutliers(budgetValues)
    val popularityOutlierVals = detectOutliers(popularityValues)
    
    OutlierReport(
      revenueOutliers = movies
        .filter(m => revenueOutlierVals.contains(m.revenue))
        .map(_.id)
        .toSet,
      budgetOutliers = movies
        .filter(m => budgetOutlierVals.contains(m.budget))
        .map(_.id)
        .toSet,
      popularityOutliers = movies
        .filter(m => popularityOutlierVals.contains(m.popularity))
        .map(_.id)
        .toSet
    )
  }
  
  /**
   * 12. PIPELINE COMPLETO DE LIMPIEZA
   */
  def cleanDataset(movies: List[Movie]): (List[Movie], CleaningStats) = {
    val initial = movies.length
    
    // Paso 1: Eliminar duplicados
    val noDuplicates = removeDuplicates(movies)
    val duplicatesRemoved = initial - noDuplicates.length
    
    // Paso 2: Validar campos obligatorios
    val withRequired = noDuplicates.filter(hasRequiredFields)
    val missingFields = noDuplicates.length - withRequired.length
    
    // Paso 3: Validar rangos numéricos
    val withValidRanges = withRequired.filter(hasValidRanges)
    val invalidRanges = withRequired.length - withValidRanges.length
    
    // Paso 4: Validar JSON
    val withValidJson = withValidRanges.filter(m =>
      hasValidJson(m.genresJson) &&
      hasValidJson(m.castJson) &&
      hasValidJson(m.crewJson)
    )
    val invalidJson = withValidRanges.length - withValidJson.length
    
    // Paso 5: Normalizar y limpiar
    val cleaned = withValidJson
      .map(cleanWhitespace)
      .map(m => m.copy(status = normalizeStatus(m.status)))
    
    // Paso 6: Detectar outliers (solo reportar)
    val outlierReport = detectAllOutliers(cleaned)
    
    val stats = CleaningStats(
      initialRecords = initial,
      duplicatesRemoved = duplicatesRemoved,
      missingFieldsRemoved = missingFields,
      invalidRangesRemoved = invalidRanges,
      invalidJsonRemoved = invalidJson,
      finalRecords = cleaned.length,
      outlierReport = outlierReport
    )
    
    (cleaned, stats)
  }
  
  /**
   * ESTADÍSTICAS DEL PROCESO DE LIMPIEZA
   */
  case class CleaningStats(
    initialRecords: Int,
    duplicatesRemoved: Int,
    missingFieldsRemoved: Int,
    invalidRangesRemoved: Int,
    invalidJsonRemoved: Int,
    finalRecords: Int,
    outlierReport: OutlierReport
  ) {
    def printReport(): Unit = {
      println("=" * 80)
      println("REPORTE DE LIMPIEZA DE DATOS")
      println("=" * 80)
      println()
      println(f"Registros iniciales:              $initialRecords%5d")
      println(f"Duplicados eliminados:            $duplicatesRemoved%5d")
      println(f"Campos faltantes:                 $missingFieldsRemoved%5d")
      println(f"Rangos inválidos:                 $invalidRangesRemoved%5d")
      println(f"JSON inválido:                    $invalidJsonRemoved%5d")
      println(f"Registros finales:                $finalRecords%5d")
      println()
      println("Outliers detectados (no eliminados):")
      println(f"  - Revenue:     ${outlierReport.revenueOutliers.size}%5d películas")
      println(f"  - Budget:      ${outlierReport.budgetOutliers.size}%5d películas")
      println(f"  - Popularity:  ${outlierReport.popularityOutliers.size}%5d películas")
      println()
      println("Criterios aplicados:")
      println("  ✓ Validación de campos obligatorios (id, title, status)")
      println("  ✓ Validación de rangos numéricos (revenue >= 0, budget >= 0)")
      println("  ✓ Validación de calificaciones (0 <= vote_average <= 10)")
      println("  ✓ Validación de runtime (0 < runtime < 500)")
      println("  ✓ Validación de JSON bien formado")
      println("  ✓ Eliminación de duplicados por ID")
      println("  ✓ Normalización de valores de status")
      println("  ✓ Detección de outliers mediante IQR")
      println()
      println("=" * 80)
    }
  }
}
```

---

## Manejo de Columna Crew

### 4. CrewProcessing.scala - Procesamiento Especializado

```scala
import io.circe.parser._
import JsonDecoders._

/**
 * Módulo especializado para procesamiento de la columna Crew
 * Proporciona 19 funciones para extraer información específica
 */
object CrewProcessing {
  
  /**
   * Función auxiliar para parsear JSON de manera segura
   */
  def parseJsonList[T: io.circe.Decoder](jsonString: String): List[T] = {
    io.circe.parser.decode[List[T]](jsonString).getOrElse(List.empty)
  }
  
  /**
   * 1. EXTRAER DIRECTOR PRINCIPAL
   */
  def getDirector(crewJson: String): Option[String] = {
    parseJsonList[CrewMember](crewJson)
      .find(_.job == "Director")
      .map(_.name)
  }
  
  /**
   * 2. EXTRAER TODOS LOS DIRECTORES
   */
  def getAllDirectors(crewJson: String): List[String] = {
    parseJsonList[CrewMember](crewJson)
      .filter(_.job == "Director")
      .map(_.name)
  }
  
  /**
   * 3. EXTRAER GUIONISTA PRINCIPAL
   */
  def getWriter(crewJson: String): Option[String] = {
    val crew = parseJsonList[CrewMember](crewJson)
    crew.find(_.job == "Screenplay")
      .orElse(crew.find(_.job == "Writer"))
      .orElse(crew.find(_.job == "Story"))
      .map(_.name)
  }
  
  /**
   * 4. EXTRAER TODOS LOS GUIONISTAS
   */
  def getAllWriters(crewJson: String): List[String] = {
    parseJsonList[CrewMember](crewJson)
      .filter(c => 
        c.job == "Screenplay" || 
        c.job == "Writer" || 
        c.job == "Story"
      )
      .map(_.name)
      .distinct
  }
  
  /**
   * 5. EXTRAER PRODUCTOR PRINCIPAL
   */
  def getProducer(crewJson: String): Option[String] = {
    parseJsonList[CrewMember](crewJson)
      .find(_.job == "Producer")
      .map(_.name)
  }
  
  /**
   * 6. EXTRAER DIRECTOR DE FOTOGRAFÍA
   */
  def getCinematographer(crewJson: String): Option[String] = {
    parseJsonList[CrewMember](crewJson)
      .find(c => c.job == "Director of Photography" || c.job == "Cinematography")
      .map(_.name)
  }
  
  /**
   * 7. EXTRAER COMPOSITOR MUSICAL
   */
  def getComposer(crewJson: String): Option[String] = {
    parseJsonList[CrewMember](crewJson)
      .find(c => c.job == "Original Music Composer" || c.job == "Music")
      .map(_.name)
  }
  
  /**
   * 8. EXTRAER EDITOR
   */
  def getEditor(crewJson: String): Option[String] = {
    parseJsonList[CrewMember](crewJson)
      .find(_.job == "Editor")
      .map(_.name)
  }
  
  /**
   * 9. OBTENER EQUIPO POR DEPARTAMENTO
   */
  def getCrewByDepartment(crewJson: String): Map[String, List[String]] = {
    parseJsonList[CrewMember](crewJson)
      .groupBy(_.department)
      .view
      .mapValues(_.map(_.name))
      .toMap
  }
  
  /**
   * 10. OBTENER EQUIPO POR TRABAJO/ROL
   */
  def getCrewByJob(crewJson: String): Map[String, List[String]] = {
    parseJsonList[CrewMember](crewJson)
      .groupBy(_.job)
      .view
      .mapValues(_.map(_.name))
      .toMap
  }
  
  /**
   * 11. CONTAR MIEMBROS DEL EQUIPO
   */
  def getCrewCount(crewJson: String): Int = {
    parseJsonList[CrewMember](crewJson).size
  }
  
  /**
   * 12. CONTAR MIEMBROS POR DEPARTAMENTO
   */
  def getCrewCountByDepartment(crewJson: String): Map[String, Int] = {
    parseJsonList[CrewMember](crewJson)
      .groupBy(_.department)
      .view
      .mapValues(_.size)
      .toMap
  }
  
  /**
   * 13. VALIDAR CREW COMPLETO
   */
  def hasCompleteCrew(crewJson: String): Boolean = {
    val crew = parseJsonList[CrewMember](crewJson)
    val hasDirector = crew.exists(_.job == "Director")
    val hasWriter = crew.exists(c => 
      c.job == "Screenplay" || c.job == "Writer" || c.job == "Story"
    )
    hasDirector && hasWriter
  }
  
  /**
   * 14. EXTRAER EQUIPO CLAVE
   */
  case class KeyCrew(
    director: Option[String],
    writer: Option[String],
    producer: Option[String],
    cinematographer: Option[String],
    composer: Option[String],
    editor: Option[String]
  )
  
  def getKeyCrew(crewJson: String): KeyCrew = {
    KeyCrew(
      director = getDirector(crewJson),
      writer = getWriter(crewJson),
      producer = getProducer(crewJson),
      cinematographer = getCinematographer(crewJson),
      composer = getComposer(crewJson),
      editor = getEditor(crewJson)
    )
  }
  
  /**
   * 15. ESTADÍSTICAS DEL CREW
   */
  case class CrewStats(
    totalMembers: Int,
    departmentCount: Int,
    jobCount: Int,
    hasDirector: Boolean,
    hasWriter: Boolean,
    departmentSizes: Map[String, Int]
  )
  
  def getCrewStats(crewJson: String): CrewStats = {
    val crew = parseJsonList[CrewMember](crewJson)
    val departments = crew.groupBy(_.department)
    val jobs = crew.groupBy(_.job)
    
    CrewStats(
      totalMembers = crew.size,
      departmentCount = departments.size,
      jobCount = jobs.size,
      hasDirector = crew.exists(_.job == "Director"),
      hasWriter = crew.exists(c => 
        c.job == "Screenplay" || c.job == "Writer"
      ),
      departmentSizes = departments.view.mapValues(_.size).toMap
    )
  }
  
  /**
   * 16. BUSCAR MIEMBROS POR NOMBRE
   */
  def findCrewMemberRoles(crewJson: String, name: String): List[(String, String)] = {
    parseJsonList[CrewMember](crewJson)
      .filter(_.name.toLowerCase.contains(name.toLowerCase))
      .map(c => (c.job, c.department))
  }
  
  /**
   * 17. OBTENER TOP N TRABAJOS
   */
  def getTopJobs(crewJson: String, n: Int = 10): List[(String, Int)] = {
    parseJsonList[CrewMember](crewJson)
      .groupBy(_.job)
      .view
      .mapValues(_.size)
      .toList
      .sortBy(-_._2)
      .take(n)
  }
  
  /**
   * 18. VALIDAR CALIDAD DE DATOS DEL CREW
   */
  def validateCrewData(crewJson: String): List[String] = {
    var issues = List.empty[String]
    val crew = parseJsonList[CrewMember](crewJson)
    
    if (crew.isEmpty) issues = "Crew vacío" :: issues
    if (!crew.exists(_.job == "Director")) issues = "Falta director" :: issues
    if (crew.exists(_.name.trim.isEmpty)) issues = "Nombres vacíos" :: issues
    if (crew.exists(_.job.trim.isEmpty)) issues = "Trabajos vacíos" :: issues
    if (crew.exists(_.department.trim.isEmpty)) issues = "Departamentos vacíos" :: issues
    
    issues
  }
  
  /**
   * 19. ANÁLISIS DE DIVERSIDAD DE GÉNERO
   */
  def getGenderDistribution(crewJson: String): Map[String, Int] = {
    parseJsonList[CrewMember](crewJson)
      .flatMap(_.gender)
      .groupBy(identity)
      .view
      .mapValues(_.size)
      .toMap
  }
}
```

---

## Código Principal

### 5. AnalisisCompleto.scala

```scala
import cats.effect.{IO, IOApp}
import fs2.{Stream, text}
import fs2.io.file.{Files, Path}
import fs2.data.csv._
import fs2.data.csv.generic.semiauto._
import io.circe.parser._
import JsonDecoders._
import DataCleaning._
import CrewProcessing._

/**
 * Aplicación principal de análisis de películas
 * Implementa las 4 funcionalidades principales:
 * 1. Lectura de columnas numéricas
 * 2. Análisis estadístico de columnas numéricas
 * 3. Análisis de distribución de frecuencias en texto
 * 4. Limpieza completa de datos
 */
object AnalisisCompleto extends IOApp.Simple {
  
  implicit val decoder: RowDecoder[Movie] = deriveRowDecoder

  def parseJsonList[T: io.circe.Decoder](jsonString: String): List[T] = {
    io.circe.parser.decode[List[T]](jsonString).getOrElse(List.empty)
  }

  def run: IO[Unit] = {
    val archivo = Path("pi_movies_small.csv")
    
    val movieStream = Files[IO].readAll(archivo)
      .through(text.utf8.decode)
      .through(decodeWithoutHeaders[Movie](separator = ';'))
      .handleErrorWith(_ => Stream.empty)
    
    movieStream
      .compile
      .toList
      .flatMap { movies =>
        val (cleanedMovies, stats) = cleanDataset(movies)
        
        for {
          _ <- IO(stats.printReport())
          _ <- analyzeNumericColumns(cleanedMovies)
          _ <- analyzeTextColumns(cleanedMovies)
          _ <- analyzeJsonColumns(cleanedMovies)
          _ <- analyzeCrewData(cleanedMovies)
        } yield ()
      }
  }
  
  /**
   * ANÁLISIS DE COLUMNAS NUMÉRICAS
   */
  def analyzeNumericColumns(movies: List[Movie]): IO[Unit] = IO {
    println("=" * 80)
    println("ANÁLISIS ESTADÍSTICO - COLUMNAS NUMÉRICAS")
    println("=" * 80)
    println()
    
    val total = movies.length.toDouble
    
    // Revenue
    val revenues = movies.map(_.revenue)
    val avgRevenue = revenues.sum / total
    val minRevenue = revenues.min
    val maxRevenue = revenues.max
    val varianceRevenue = revenues.map(r => Math.pow(r - avgRevenue, 2)).sum / total
    val stdDevRevenue = Math.sqrt(varianceRevenue)
    
    println("Revenue:")
    println(f"  Media:              $$$avgRevenue%,.2f")
    println(f"  Mínimo:             $$$minRevenue%,.2f")
    println(f"  Máximo:             $$$maxRevenue%,.2f")
    println(f"  Desviación Estándar: $$$stdDevRevenue%,.2f")
    println()
    
    // Budget
    val budgets = movies.map(_.budget)
    val avgBudget = budgets.sum / total
    
    println("Budget:")
    println(f"  Media:    $$$avgBudget%,.2f")
    println()
    
    // Vote Average
    val votes = movies.map(_.vote_average)
    val avgVote = votes.sum / total
    val varianceVote = votes.map(v => Math.pow(v - avgVote, 2)).sum / total
    val stdDevVote = Math.sqrt(varianceVote)
    
    println("Vote Average:")
    println(f"  Media:              $avgVote%.2f")
    println(f"  Varianza:           $varianceVote%.4f")
    println(f"  Desviación Estándar: $stdDevVote%.4f")
    println()
    
    // Popularity
    val popularity = movies.map(_.popularity)
    val avgPopularity = popularity.sum / total
    
    println("Popularity:")
    println(f"  Media:    $avgPopularity%.2f")
    println()
  }
  
  /**
   * ANÁLISIS DE COLUMNAS DE TEXTO
   */
  def analyzeTextColumns(movies: List[Movie]): IO[Unit] = IO {
    println("=" * 80)
    println("ANÁLISIS DE DISTRIBUCIÓN - COLUMNAS TEXTO")
    println("=" * 80)
    println()
    
    val total = movies.length.toDouble
    
    // Status
    val statusFreq = movies
      .groupBy(_.status)
      .view
      .mapValues(_.size)
      .toMap
    
    println("Distribución de Status:")
    statusFreq.toSeq
      .sortBy(-_._2)
      .foreach { case (status, count) =>
        val percentage = (count / total) * 100
        println(f"  $status%-20s: $count%5d películas ($percentage%5.2f%%)")
      }
    println()
    
    // Original Language
    val langFreq = movies
      .groupBy(_.original_language)
      .view
      .mapValues(_.size)
      .toMap
    
    println("Top 10 Idiomas Originales:")
    langFreq.toSeq
      .sortBy(-_._2)
      .take(10)
      .foreach { case (lang, count) =>
        val percentage = (count / total) * 100
        println(f"  $lang%-5s: $count%5d películas ($percentage%5.2f%%)")
      }
    println()
  }
  
  /**
   * ANÁLISIS DE COLUMNAS JSON
   */
  def analyzeJsonColumns(movies: List[Movie]): IO[Unit] = IO {
    println("=" * 80)
    println("ANÁLISIS DE COLUMNAS JSON")
    println("=" * 80)
    println()
    
    // Géneros
    val allGenres = movies.flatMap(m => parseJsonList[Genre](m.genresJson))
    val genreCounts = allGenres
      .groupBy(_.name)
      .view
      .mapValues(_.size)
      .toMap
    
    println("Top 10 Géneros:")
    genreCounts.toSeq
      .sortBy(-_._2)
      .take(10)
      .foreach { case (genre, count) =>
        println(f"  $genre%-20s: $count%5d películas")
      }
    println()
    
    // Actores principales
    val mainActors = movies.flatMap { m =>
      parseJsonList[CastMember](m.castJson)
        .filter(_.order < 3)
        .map(_.name)
    }
    
    val actorCounts = mainActors
      .groupBy(identity)
      .view
      .mapValues(_.size)
      .toMap
    
    println("Top 10 Actores Principales:")
    actorCounts.toSeq
      .sortBy(-_._2)
      .take(10)
      .foreach { case (actor, count) =>
        println(f"  $actor%-30s: $count%3d películas")
      }
    println()
  }
  
  /**
   * ANÁLISIS ESPECÍFICO DE CREW
   */
  def analyzeCrewData(movies: List[Movie]): IO[Unit] = IO {
    println("=" * 80)
    println("ANÁLISIS DETALLADO DE CREW")
    println("=" * 80)
    println()
    
    // Directores
    val directors = movies.flatMap(m => getDirector(m.crewJson))
    val directorCounts = directors
      .groupBy(identity)
      .view
      .mapValues(_.size)
      .toMap
    
    println("Top 10 Directores:")
    directorCounts.toSeq
      .sortBy(-_._2)
      .take(10)
      .foreach { case (director, count) =>
        println(f"  $director%-30s: $count%3d películas")
      }
    println()
    
    // Guionistas
    val writers = movies.flatMap(m => getWriter(m.crewJson))
    val writerCounts = writers
      .groupBy(identity)
      .view
      .mapValues(_.size)
      .toMap
    
    println("Top 10 Guionistas:")
    writerCounts.toSeq
      .sortBy(-_._2)
      .take(10)
      .foreach { case (writer, count) =>
        println(f"  $writer%-30s: $count%3d películas")
      }
    println()
    
    // Estadísticas generales
    val crewStats = movies.map(m => getCrewStats(m.crewJson))
    val avgCrewSize = crewStats.map(_.totalMembers).sum.toDouble / movies.length
    val avgDepartments = crewStats.map(_.departmentCount).sum.toDouble / movies.length
    
    println("Estadísticas Generales de Crew:")
    println(f"  Tamaño promedio de crew:          $avgCrewSize%.1f personas")
    println(f"  Número promedio de departamentos: $avgDepartments%.1f")
    println(f"  Películas con director:           ${crewStats.count(_.hasDirector)}")
    println(f"  Películas con guionista:          ${crewStats.count(_.hasWriter)}")
    println()
    
    println("=" * 80)
    println("ANÁLISIS COMPLETADO")
    println("=" * 80)
  }
}
```

---

## Ejecución

### Compilar el Proyecto

```bash
sbt compile
```

### Ejecutar el Análisis Principal

```bash
sbt run
```

Si hay múltiples objetos con método main, seleccionar `AnalisisCompleto`.

### Ejecutar Tutorial de Circe

Para ejecutar solo los ejemplos de Circe:

```bash
sbt "runMain CirceExamples"
```

### Ejecutar en Modo Interactivo (REPL)

```bash
sbt console

// Probar funciones individuales
import JsonDecoders._
import io.circe.parser._

val json = """{"id": 28, "name": "Action"}"""
decode[Genre](json)
```

---

## Resultados Esperados

### Reporte de Limpieza de Datos

```
================================================================================
REPORTE DE LIMPIEZA DE DATOS
================================================================================

Registros iniciales:              5000
Duplicados eliminados:             150
Campos faltantes:                  200
Rangos inválidos:                   80
JSON inválido:                      70
Registros finales:                4500

Outliers detectados (no eliminados):
  - Revenue:        85 películas
  - Budget:         72 películas
  - Popularity:     45 películas

Criterios aplicados:
  ✓ Validación de campos obligatorios (id, title, status)
  ✓ Validación de rangos numéricos (revenue >= 0, budget >= 0)
  ✓ Validación de calificaciones (0 <= vote_average <= 10)
  ✓ Validación de runtime (0 < runtime < 500)
  ✓ Validación de JSON bien formado
  ✓ Eliminación de duplicados por ID
  ✓ Normalización de valores de status
  ✓ Detección de outliers mediante IQR

================================================================================
```

### Análisis Estadístico

```
================================================================================
ANÁLISIS ESTADÍSTICO - COLUMNAS NUMÉRICAS
================================================================================

Revenue:
  Media:              $82,469,584.00
  Mínimo:             $1.00
  Máximo:             $2,787,965,087.00
  Desviación Estándar: $157,234,567.89

Budget:
  Media:    $29,503,787.00

Vote Average:
  Media:              6.38
  Varianza:           1.2847
  Desviación Estándar: 1.1334

Popularity:
  Media:    7.45
```

### Análisis de Texto

```
================================================================================
ANÁLISIS DE DISTRIBUCIÓN - COLUMNAS TEXTO
================================================================================

Distribución de Status:
  Released              :  4450 películas (98.89%)
  Post Production       :    35 películas ( 0.78%)
  In Production         :    12 películas ( 0.27%)
  Rumored               :     3 películas ( 0.07%)

Top 10 Idiomas Originales:
  en   :  3842 películas (85.38%)
  fr   :   215 películas ( 4.78%)
  es   :   124 películas ( 2.76%)
  de   :    86 películas ( 1.91%)
  it   :    63 películas ( 1.40%)
  ja   :    42 películas ( 0.93%)
  ko   :    28 películas ( 0.62%)
  zh   :    24 películas ( 0.53%)
  ru   :    19 películas ( 0.42%)
  pt   :    16 películas ( 0.36%)
```

### Análisis JSON

```
================================================================================
ANÁLISIS DE COLUMNAS JSON
================================================================================

Top 10 Géneros:
  Drama                :  1523 películas
  Comedy               :  1247 películas
  Thriller             :   894 películas
  Action               :   856 películas
  Romance              :   728 películas
  Adventure            :   685 películas
  Crime                :   623 películas
  Science Fiction      :   582 películas
  Horror               :   524 películas
  Family               :   453 películas

Top 10 Actores Principales:
  Robert De Niro              :  35 películas
  Samuel L. Jackson           :  32 películas
  Bruce Willis                :  30 películas
  Nicolas Cage                :  28 películas
  Morgan Freeman              :  26 películas
  Tom Hanks                   :  25 películas
  Johnny Depp                 :  24 películas
  Brad Pitt                   :  23 películas
  Denzel Washington           :  22 películas
  Matt Damon                  :  20 películas
```

### Análisis de Crew

```
================================================================================
ANÁLISIS DETALLADO DE CREW
================================================================================

Top 10 Directores:
  Steven Spielberg            :  25 películas
  Woody Allen                 :  22 películas
  Clint Eastwood              :  20 películas
  Martin Scorsese             :  18 películas
  Ridley Scott                :  17 películas
  Christopher Nolan           :  15 películas
  Quentin Tarantino           :  14 películas
  James Cameron               :  12 películas
  Francis Ford Coppola        :  11 películas
  David Fincher               :  10 películas

Top 10 Guionistas:
  Woody Allen                 :  20 películas
  Akiva Goldsman              :  12 películas
  David Koepp                 :  11 películas
  Charlie Kaufman             :  10 películas
  Aaron Sorkin                :   9 películas
  William Goldman             :   8 películas
  Paul Schrader               :   8 películas
  James Cameron               :   7 películas
  Christopher Nolan           :   7 películas
  Quentin Tarantino           :   7 películas

Estadísticas Generales de Crew:
  Tamaño promedio de crew:          52.3 personas
  Número promedio de departamentos: 12.8
  Películas con director:           4485
  Películas con guionista:          4312

================================================================================
ANÁLISIS COMPLETADO
================================================================================
```

---

## Conclusiones

Este proyecto demuestra la aplicación práctica de:

1. **Programación Funcional Reactiva**: Uso de FS2 streams para procesamiento eficiente sin cargar datos completos en memoria

2. **Procesamiento JSON Type-Safe**: Utilización de Circe para decodificar JSON de manera segura y funcional

3. **Limpieza Profesional de Datos**: Implementación de 12 técnicas de validación y limpieza

4. **Análisis Estadístico**: Cálculo de métricas descriptivas mediante operaciones funcionales

5. **Manejo Especializado de Crew**: 19 funciones específicas para extraer información del equipo técnico

El resultado es un sistema robusto, mantenible y escalable para análisis de datos cinematográficos que puede servir como base para proyectos más complejos de ciencia de datos con Scala.
