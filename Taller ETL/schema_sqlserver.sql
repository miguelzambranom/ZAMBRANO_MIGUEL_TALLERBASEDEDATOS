-- ===============================================================================
-- ESQUEMA DDL PARA SQL SERVER (SQL Server Management Studio - SSMS)
-- Base de datos: TallerETL_Clinica
-- ===============================================================================
-- Ejecutar en SSMS conectado al servidor local:
--   1. Crear la BD (si no existe): ver setup_sqlserver.py
--   2. Seleccionar la BD TallerETL_Clinica
--   3. Ejecutar este script (F5)
-- ===============================================================================

USE TallerETL_Clinica;
GO

IF OBJECT_ID('dbo.admisiones_emergencia', 'U') IS NOT NULL
    DROP TABLE dbo.admisiones_emergencia;
GO

CREATE TABLE dbo.admisiones_emergencia (
    id_admision       VARCHAR(36)      NOT NULL PRIMARY KEY,
    fecha_ingreso     DATETIME2        NOT NULL,
    id_paciente       VARCHAR(10)      NOT NULL,
    cama_asignada     VARCHAR(30)      NOT NULL,
    diagnostico       VARCHAR(100)     NOT NULL,
    costo_consulta    DECIMAL(10, 2)   NOT NULL,
    comision_seguro   DECIMAL(10, 2)   NOT NULL,
    costo_neto        DECIMAL(10, 2)   NOT NULL,
    alerta_gravedad   BIT              NOT NULL DEFAULT 0,
    estado_paciente   VARCHAR(20)      NOT NULL,
    fecha_carga       DATETIME2        NOT NULL DEFAULT SYSDATETIME()
);
GO

CREATE INDEX idx_admisiones_fecha      ON dbo.admisiones_emergencia (fecha_ingreso);
CREATE INDEX idx_admisiones_paciente   ON dbo.admisiones_emergencia (id_paciente);
CREATE INDEX idx_admisiones_diagnostico ON dbo.admisiones_emergencia (diagnostico);
GO

-- Consultas útiles para verificar la carga en SSMS:
-- SELECT COUNT(*) AS total FROM dbo.admisiones_emergencia;
-- SELECT TOP 100 * FROM dbo.admisiones_emergencia ORDER BY fecha_ingreso DESC;
-- SELECT * FROM dbo.admisiones_emergencia WHERE alerta_gravedad = 1;
