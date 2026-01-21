-- ============================================================
-- ANALYTICAL QUERIES – PUBLIC HEALTH HOSPITAL ANALYTICS
-- MSc in Data Science (ITBA) – Data Warehouses & OLAP
-- Schema: PilarSalud
-- ============================================================


-- 1. Ausentismo de pacientes por especialidad médica,
--    diferenciando pacientes con y sin cobertura médica
WITH AusentismoXEspecialidad_OS AS (
    SELECT
        e."NombreEspecialidad",
        p."TieneOS",
        COUNT(*) AS cantidad
    FROM "PilarSalud"."Turnos" t
    JOIN "PilarSalud"."EspecialidadMedico" em ON t."Id_EspMedico" = em."Id_EspMedico"
    JOIN "PilarSalud"."Especialidad" e ON e."Id_Especialidad" = em."Id_Especialidad"
    JOIN "PilarSalud"."Paciente" p ON t."Id_Paciente" = p."Nro_Documento"
    WHERE UPPER(t."EstadoTurno") = 'AUSENTE'
    GROUP BY e."NombreEspecialidad", p."TieneOS"
)
SELECT
    "NombreEspecialidad" AS "Especialidad",
    "TieneOS" AS "Tiene cobertura médica",
    cantidad AS "Cantidad de ausentismos",
    ROUND(
        100.0 * cantidad /
        NULLIF(SUM(cantidad) OVER (PARTITION BY "NombreEspecialidad"), 0),
        2
    ) AS "Porcentaje por especialidad"
FROM AusentismoXEspecialidad_OS
ORDER BY "NombreEspecialidad", "TieneOS";


-- 2. Evolución de la demanda de turnos por especialidad
--    comparando con el mes anterior
WITH Meses AS (
    SELECT DISTINCT "Mes"
    FROM "PilarSalud"."Fecha"
),
Especialidades AS (
    SELECT DISTINCT "NombreEspecialidad"
    FROM "PilarSalud"."Especialidad"
),
EspXMes AS (
    SELECT *
    FROM Meses CROSS JOIN Especialidades
),
DemandaXEspecialidad AS (
    SELECT
        COUNT(t."Id_Turno") AS demanda,
        e."NombreEspecialidad",
        f."Mes"
    FROM "PilarSalud"."Turnos" t
    JOIN "PilarSalud"."EspecialidadMedico" em ON t."Id_EspMedico" = em."Id_EspMedico"
    JOIN "PilarSalud"."Especialidad" e ON e."Id_Especialidad" = em."Id_Especialidad"
    JOIN "PilarSalud"."Fecha" f ON t."FechaTurno" = f."Id_Fecha"
    GROUP BY e."NombreEspecialidad", f."Mes"
)
SELECT
    em."Mes",
    em."NombreEspecialidad",
    COALESCE(d.demanda, 0) AS "Demanda",
    LAG(COALESCE(d.demanda, 0), 1)
        OVER (PARTITION BY em."NombreEspecialidad" ORDER BY em."Mes")
        AS "Demanda mes previo"
FROM EspXMes em
LEFT JOIN DemandaXEspecialidad d
    ON em."NombreEspecialidad" = d."NombreEspecialidad"
   AND em."Mes" = d."Mes"
ORDER BY em."NombreEspecialidad", em."Mes";


-- 3. Atención médica en el sector público
--    de pacientes con cobertura médica
SELECT
    COUNT(t."Id_Paciente") AS "Pacientes atendidos con cobertura"
FROM "PilarSalud"."Turnos" t
JOIN "PilarSalud"."Paciente" p
    ON t."Id_Paciente" = p."Nro_Documento"
WHERE p."TieneOS" = true;


-- 4. Relación entre tiempo de espera y porcentaje de asistencia
WITH FechasTurnos AS (
    SELECT
        t."Id_Turno",
        t."EstadoTurno",
        MAKE_DATE(f1."Año", f1."Mes", f1."Dia") AS fecha_solicitud,
        MAKE_DATE(f2."Año", f2."Mes", f2."Dia") AS fecha_turno
    FROM "PilarSalud"."Turnos" t
    JOIN "PilarSalud"."Fecha" f1 ON t."FechaSolicitud" = f1."Id_Fecha"
    JOIN "PilarSalud"."Fecha" f2 ON t."FechaTurno" = f2."Id_Fecha"
),
TurnosConEspera AS (
    SELECT
        "Id_Turno",
        "EstadoTurno",
        (fecha_turno - fecha_solicitud) AS dias_espera,
        CASE
            WHEN (fecha_turno - fecha_solicitud) < 0 THEN 'Error registro'
            WHEN (fecha_turno - fecha_solicitud) <= 7 THEN '0-7 días'
            WHEN (fecha_turno - fecha_solicitud) <= 14 THEN '8-14 días'
            WHEN (fecha_turno - fecha_solicitud) <= 30 THEN '15-30 días'
            ELSE '>30 días'
        END AS rango_espera
    FROM FechasTurnos
)
SELECT
    rango_espera,
    COUNT(*) AS total_turnos,
    SUM(CASE WHEN "EstadoTurno" = 'Atendido' THEN 1 ELSE 0 END) AS turnos_asistidos,
    ROUND(
        100.0 * SUM(CASE WHEN "EstadoTurno" = 'Atendido' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS porcentaje_asistencia
FROM TurnosConEspera
GROUP BY rango_espera
ORDER BY rango_espera;

-- 5) Especialidades con mayor cantidad de turnos asignados:
--    incluye cantidad de médicos, turnos atendidos y carga promedio por médico
WITH MedicosPorEspecialidad AS (
    SELECT
        e."NombreEspecialidad",
        COUNT(DISTINCT em."Documento") AS cantidad_medicos
    FROM "PilarSalud"."EspecialidadMedico" em
    JOIN "PilarSalud"."Especialidad" e
        ON e."Id_Especialidad" = em."Id_Especialidad"
    GROUP BY e."NombreEspecialidad"
)
SELECT
    e."NombreEspecialidad",
    COUNT(*) AS total_turnos,
    SUM(CASE WHEN t."EstadoTurno" = 'Atendido' THEN 1 ELSE 0 END) AS turnos_atendidos,
    m.cantidad_medicos,
    ROUND(COUNT(*)::numeric / NULLIF(m.cantidad_medicos, 0), 2) AS turnos_promedio_por_medico
FROM "PilarSalud"."Turnos" t
JOIN "PilarSalud"."EspecialidadMedico" em
    ON t."Id_EspMedico" = em."Id_EspMedico"
JOIN "PilarSalud"."Especialidad" e
    ON e."Id_Especialidad" = em."Id_Especialidad"
JOIN MedicosPorEspecialidad m
    ON m."NombreEspecialidad" = e."NombreEspecialidad"
GROUP BY e."NombreEspecialidad", m.cantidad_medicos
ORDER BY turnos_promedio_por_medico DESC;



-- 6) Gasto en medicamentos (pacientes con cobertura médica) mes a mes:
--    incluye gasto mensual, acumulado anual (YTD) y promedio acumulado
WITH RecetadosOS AS (
    SELECT
        t."Id_Turno",
        t."Id_Receta",
        t."Id_Paciente"
    FROM "PilarSalud"."Turnos" t
    JOIN "PilarSalud"."Paciente" p
        ON t."Id_Paciente" = p."Nro_Documento"
    WHERE p."TieneOS" = true
      AND t."Id_Receta" IS NOT NULL
),
GastoXMes AS (
    SELECT
        pac."Id_Paciente",
        f."Año",
        f."Mes",
        SUM(i."PrecioMedicamento") AS "GastoMes"
    FROM RecetadosOS pac
    JOIN "PilarSalud"."Receta" r
        ON pac."Id_Receta" = r."Id_Receta"
    JOIN "PilarSalud"."Indica" i
        ON i."Id_Receta" = r."Id_Receta"
    JOIN "PilarSalud"."Fecha" f
        ON f."Id_Fecha" = r."Id_FechaReceta"
    GROUP BY pac."Id_Paciente", f."Año", f."Mes"
)
SELECT
    "Id_Paciente",
    "Año",
    "Mes",
    "GastoMes",
    SUM("GastoMes") OVER (
        PARTITION BY "Id_Paciente", "Año"
        ORDER BY "Mes"
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS "Gasto Acumulado Anual",
    AVG("GastoMes") OVER (
        PARTITION BY "Id_Paciente", "Año"
        ORDER BY "Mes"
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS "Promedio Acumulado Anual"
FROM GastoXMes
ORDER BY "Id_Paciente", "Año", "Mes";



-- 7) Especialidades que más medicamentos recetan y su % del gasto total
WITH RecetaXEspecialidad AS (
    SELECT
        t."Id_Receta",
        e."NombreEspecialidad"
    FROM "PilarSalud"."Turnos" t
    JOIN "PilarSalud"."EspecialidadMedico" em
        ON t."Id_EspMedico" = em."Id_EspMedico"
    JOIN "PilarSalud"."Especialidad" e
        ON e."Id_Especialidad" = em."Id_Especialidad"
    WHERE t."Id_Receta" IS NOT NULL
),
GastoXEspecialidad AS (
    SELECT
        re."NombreEspecialidad",
        COUNT(i."Id_Medicamento") AS "CantidadMedicamentosRecetados",
        SUM(i."PrecioMedicamento") AS "GastoEnMedicamentos"
    FROM RecetaXEspecialidad re
    JOIN "PilarSalud"."Indica" i
        ON re."Id_Receta" = i."Id_Receta"
    GROUP BY re."NombreEspecialidad"
)
SELECT
    "NombreEspecialidad",
    "CantidadMedicamentosRecetados",
    "GastoEnMedicamentos",
    ROUND(
        100.0 * "GastoEnMedicamentos" / NULLIF(SUM("GastoEnMedicamentos") OVER (), 0),
        3
    ) AS "PorcentajeGastoTotal"
FROM GastoXEspecialidad
ORDER BY "CantidadMedicamentosRecetados" DESC;



-- 8) Por receta: costo total, monto devuelto por SAMO, fechas y demora en días
WITH PrecioReceta AS (
    SELECT
        r."Id_Receta" AS "Id_Receta",
        SUM(i."PrecioMedicamento") AS "PrecioReceta",
        f."Dia" AS "DiaRec",
        f."Mes" AS "MesRec",
        f."Año" AS "AnioRec"
    FROM "PilarSalud"."Indica" i
    JOIN "PilarSalud"."Receta" r
        ON i."Id_Receta" = r."Id_Receta"
    JOIN "PilarSalud"."Fecha" f
        ON f."Id_Fecha" = r."Id_FechaReceta"
    GROUP BY r."Id_Receta", f."Dia", f."Mes", f."Año"
),
PagosRecetas AS (
    SELECT
        pr."Id_Receta",
        pr."PrecioReceta",
        pr."DiaRec",
        pr."MesRec",
        pr."AnioRec",
        f2."Dia" AS "DiaPago",
        f2."Mes" AS "MesPago",
        f2."Año" AS "AnioPago",
        p."Monto"
    FROM PrecioReceta pr
    LEFT JOIN "PilarSalud"."Pago" p
        ON p."Id_Receta" = pr."Id_Receta"          -- LEFT JOIN: conserva recetas sin pago
    LEFT JOIN "PilarSalud"."Fecha" f2
        ON p."Id_FechaPago" = f2."Id_Fecha"
)
SELECT
    "Id_Receta",
    "PrecioReceta",
    "Monto" AS "MontoDevueltoXSAMO",
    MAKE_DATE("AnioRec", "MesRec", "DiaRec") AS "FechaReceta",
    MAKE_DATE("AnioPago", "MesPago", "DiaPago") AS "FechaPago",
    (MAKE_DATE("AnioPago", "MesPago", "DiaPago") - MAKE_DATE("AnioRec", "MesRec", "DiaRec")) AS "DiasDemora"
FROM PagosRecetas
ORDER BY "Id_Receta";



-- 9) Comparación mensual: costo histórico vs valor actualizado vs devuelto por SAMO
WITH PrecioReceta AS (
    SELECT
        r."Id_Receta" AS "Id_Receta",
        SUM(i."PrecioMedicamento") AS "PrecioReceta",
        SUM(m."PrecioActual") AS "PrecioActual"
    FROM "PilarSalud"."Indica" i
    JOIN "PilarSalud"."Receta" r
        ON i."Id_Receta" = r."Id_Receta"
    JOIN "PilarSalud"."Medicamento" m
        ON m."Id_Medicamento" = i."Id_Medicamento"
    GROUP BY r."Id_Receta"
),
PagosRecetas AS (
    SELECT
        f."Año" AS "AnioPago",
        f."Mes" AS "MesPago",
        SUM(pr."PrecioReceta") AS "PrecioReceta",
        SUM(pr."PrecioActual") AS "PrecioActual",
        SUM(p."Monto") AS "Monto"
    FROM PrecioReceta pr
    LEFT JOIN "PilarSalud"."Pago" p
        ON p."Id_Receta" = pr."Id_Receta"
    LEFT JOIN "PilarSalud"."Fecha" f
        ON p."Id_FechaPago" = f."Id_Fecha"
    GROUP BY f."Año", f."Mes"
)
SELECT
    MAKE_DATE("AnioPago", "MesPago", 1) AS "FechaPago",
    "PrecioReceta",
    "PrecioActual",
    "Monto" AS "MontoDevueltoXSAMO",
    ("PrecioReceta" - "PrecioActual") AS "DiferenciaEnPlata",
    ROUND(100.0 * ("PrecioReceta" - "PrecioActual") / NULLIF("PrecioReceta", 0), 2) AS "DiferenciaEnPorcentaje",
    SUM("PrecioReceta") OVER () AS "TotalRecetas",
    SUM("PrecioActual") OVER () AS "TotalPrecioActual",
    (SUM("PrecioReceta") OVER () - SUM("PrecioActual") OVER ()) AS "TotalDiferencia",
    ROUND(
        100.0 * (SUM("PrecioReceta") OVER () - SUM("PrecioActual") OVER ()) /
        NULLIF(SUM("PrecioReceta") OVER (), 0),
        2
    ) AS "DiferenciaTotalEnPorcentaje"
FROM PagosRecetas
ORDER BY "AnioPago", "MesPago";

-- 10) Gasto en medicamentos por especialidad y por mes
--     + acumulado anual (YTD) por especialidad
WITH TotalEspecialidad AS (
    SELECT
        e."NombreEspecialidad",
        f."Año",
        f."Mes",
        SUM(i."PrecioMedicamento") AS "TotalMedicamentos"
    FROM "PilarSalud"."Indica" i
    JOIN "PilarSalud"."Turnos" t
        ON t."Id_Receta" = i."Id_Receta"
    JOIN "PilarSalud"."Receta" r
        ON r."Id_Receta" = t."Id_Receta"
    JOIN "PilarSalud"."Fecha" f
        ON f."Id_Fecha" = r."Id_FechaReceta"
    JOIN "PilarSalud"."EspecialidadMedico" em
        ON em."Id_EspMedico" = t."Id_EspMedico"
    JOIN "PilarSalud"."Especialidad" e
        ON e."Id_Especialidad" = em."Id_Especialidad"
    GROUP BY e."NombreEspecialidad", f."Año", f."Mes"
)
SELECT
    "NombreEspecialidad",
    "Año",
    "Mes",
    "TotalMedicamentos",
    SUM("TotalMedicamentos") OVER (
        PARTITION BY "NombreEspecialidad", "Año"
        ORDER BY "Mes"
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS "YTD_Total"
FROM TotalEspecialidad
ORDER BY "NombreEspecialidad", "Año", "Mes";



-- 11) Distribución de cancelaciones por motivo
--     (porcentaje sobre el total de cancelaciones)
WITH Cancelaciones AS (
    SELECT
        m."NombreMotivo",
        COUNT(*) AS "Cantidad"
    FROM "PilarSalud"."Turnos" t
    JOIN "PilarSalud"."MotivoCancelacion" m
        ON m."Id_Motivo" = t."Id_Motivo"
    WHERE t."EstadoTurno" = 'Cancelado'
    GROUP BY m."NombreMotivo"
)
SELECT
    "NombreMotivo",
    "Cantidad",
    ROUND(100.0 * "Cantidad" / NULLIF(SUM("Cantidad") OVER (), 0), 2) AS "Porcentaje"
FROM Cancelaciones
ORDER BY "Cantidad" DESC;



-- 12) Porcentaje de cancelación por mes (y total anual de cancelaciones)
WITH CancelacionesPorMes AS (
    SELECT
        f."Mes",
        f."Año",
        COUNT(*) AS "Cantidad"
    FROM "PilarSalud"."Turnos" t
    JOIN "PilarSalud"."Fecha" f
        ON f."Id_Fecha" = t."FechaTurno"
    WHERE t."EstadoTurno" = 'Cancelado'
    GROUP BY f."Mes", f."Año"
)
SELECT
    "Mes",
    "Año",
    "Cantidad",
    ROUND(100.0 * "Cantidad" / NULLIF(SUM("Cantidad") OVER (), 0), 2) AS "PorcentajeMensual",
    SUM("Cantidad") OVER (PARTITION BY "Año") AS "TotalAnual"
FROM CancelacionesPorMes
ORDER BY "Año", "Mes";



-- 13) Efectores con mayor presentismo (turnos atendidos por dependencia)
SELECT
    d."NombreDependencia",
    COUNT(t."Id_Turno") AS "Cant_Pacientes"
FROM "PilarSalud"."Turnos" t
JOIN "PilarSalud"."Dependencia" d
    ON d."Id_Dependencia" = t."Id_Dependencia"
WHERE t."EstadoTurno" = 'Atendido'
GROUP BY d."NombreDependencia"
ORDER BY COUNT(t."Id_Turno") DESC;