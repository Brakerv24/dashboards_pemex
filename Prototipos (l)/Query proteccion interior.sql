CREATE TABLE dashboard_proteccion_interior AS
SELECT 
    m.sap_ddv_ducto, 
    m.n_ducto, 
    m.act_ger,
	m.origen,
	m.destino,
	m.diam_in,
	m.lon_km,
	m.servicio,
	m.cond_oper,
	h.lado,
	h.punto_de_evaluación,
	h.fecha_retiro,
	h.velocidad_de_corrosión_mpy,
    h.observaciones
FROM 
    id_ducto m
INNER JOIN 
    hsitorico_proteccion_interior h 
    ON m.sap_ddv_ducto = h.sap_ddv_ducto
ORDER BY 
    m.sap_ddv_ducto ASC;

