# Taller Práctico: ETL Clínico con Generadores en Python

Este documento contiene la descripción de la actividad de laboratorio, las instrucciones de entrega y la rúbrica de evaluación para el taller de ETL basado en streaming de datos clínicos.

---

## Descripción de la Actividad

### Contexto
La **Clínica San José** necesita procesar un archivo masivo de logs de admisiones médicas (`logs_admisiones_masivas.csv`) con más de 100,000 registros históricos. El servidor de integración actual tiene recursos limitados y dispone de un **máximo de 20 MB de RAM** para esta tarea. El sistema actual colapsa debido a que intenta cargar el archivo completo en memoria, provocando errores de Out-Of-Memory (OOM).

### Objetivo
Diseñar e implementar un pipeline **ETL (Extract, Transform, Load)** en Python utilizando **Generadores (`yield`)** para procesar el dataset de manera streaming (lote a lote). Esto evitará el consumo excesivo de memoria RAM y garantizará una carga eficiente en una base de datos local SQLite (o PostgreSQL como alternativa avanzada).

---

## Entregables de la Actividad

Para que la actividad sea calificada, los estudiantes deberán entregar un archivo comprimido con el nombre `Taller_ETL_Apellido_Nombre.zip` que contenga únicamente:

1. **Código fuente completo (`plantilla_taller.py`)**: Con todos los bloques `# TODO` resueltos y el pipeline funcional.
2. **Captura de pantalla del output**: Imagen de la consola que muestre el log de lotes, el tiempo total y el **Pico Máximo de Memoria RAM (< 20 MB)**.
3. **Este archivo completo (`GUIA_ACTIVIDAD_ETL.md`)**: Completado con las respuestas a las preguntas de análisis y teoría de la sección final.

---

## Rúbrica de Evaluación (Ponderación Total: 100%)

| Criterio | Ponderación | Aspecto a Evaluar |
| :--- | :---: | :--- |
| **1. Extractor con `yield`** | **20%** | Implementación correcta y perezosa en lotes exactos y control del remanente final. |
| **2. Transformación de Datos** | **25%** | Aplicación de reglas de negocio clínicas (limpieza, comisión, costo neto y alerta de gravedad) con manejo de excepciones. |
| **3. Carga en Base de Datos** | **15%** | Carga masiva eficiente (`executemany` o similar) y control de transacciones por lote. |
| **4. Monitoreo de RAM** | **10%** | Demostración de consumo de RAM constante por debajo de 20 MB validado mediante captura de pantalla. |
| **5. Calidad del Código** | **15%** | Código limpio, estructurado bajo PEP 8, modular y debidamente documentado. |
| **6. Respuestas al Cuestionario** | **15%** | Explicación clara del streaming, justificación de cargas agrupadas y respuestas fundamentadas del cuestionario de este archivo. |

---
> [!IMPORTANT]
> **Nota para el Estudiante:** El límite de consumo de memoria RAM (<20 MB) es un requerimiento crítico. Si la solución procesa los registros pero supera el límite de RAM, se penalizará severamente en los criterios de Extractor y Monitoreo.

---

## Cuestionario de Evaluación y Análisis Técnico

*Instrucciones: Los estudiantes deben responder a las siguientes preguntas en este mismo archivo, completando las secciones indicadas abajo.*

### Nombre del Estudiante:
[Escriba su nombre completo aquí]

### 1. Funcionamiento de la Evaluación Perezosa (Lazy Evaluation)
Describa cómo funciona el extractor implementado y de qué manera la instrucción `yield` en Python evita el agotamiento de memoria RAM en el servidor.
> *Respuesta:*El extracto empieza a leer la fuente de datos uno por uno y no carga todo de golpe 

### 2. Justificación de la Inserción por Lotes (Batch Loading)
Explique el impacto que tiene en el rendimiento de la base de datos agrupar los registros para su carga en lugar de insertarlos individualmente registro por registro.
> *Respuesta:*Optimiza carga evitando sobrecargas, tambien optimiza el uso del motor e indices tambien y asprovecha mas el ancho de banda al enviar una pregunta amplia en lugar de mil consultas chiquitas 

### 3. Diferencias en Memoria: `yield` vs `return`
Detalle la diferencia técnica en la asignación de memoria RAM entre una función que genera y acumula una lista en memoria (`return`) y una función generadora.
> *Respuesta:*La diferencia es que el return retiene todos los registros en la ram simultáneamente manteniendo sus referencias vivas en una lista, mientras que el yield procesa y libera cada registro , permitiendo así que el recolector limpie la memoria al instante.

### 4. Escalabilidad de la Solución
¿Por qué el consumo de memoria RAM medido en la consola se mantiene constante y bajo sin importar el tamaño total del archivo procesado (sea de 100,000 o de 10,000,000 de registros)?
> *Respuesta:*Porque el servidor solo guarda en ram el registro actual en proceso

### 5. Optimización Tecnológica (`executemany`)
¿Cuál es la diferencia de desempeño y uso de conexiones en la base de datos entre usar el método `executemany` y usar un bucle iterativo que llame individualmente a `execute`?
> *Respuesta:*Uso de conexiones: executemany mantiene una sola conexión y transacción para todo el grupo, mientras que un bucle con execute realiza múltiples viajes de red sobre la conexión, saturando la latencia de red
