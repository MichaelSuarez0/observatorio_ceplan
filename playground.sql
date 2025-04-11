-- CREATE TABLE IF NOT EXISTS log(
--     id INTEGER PRIMARY KEY AUTOINCREMENT,
--     new_name TEXT,
--     original_name TEXT,
--     path TEXT,
--     author TEXT,
--     sharepoint_uploaded INTEGER CHECK (sharepoint_uploaded IN (0,1))
-- )

INSERT INTO log(id, new_name, original_name, path, author, sharepoint_uploaded) VALUES (?, ?, ?, ?, ?, ?)

INSERT INTO merged_table (
        codigo, vistas, titulo_corto, titulo_largo)
    SELECT
        vistas.codigo,
        vistas.vistas,
        info_fichas.titulo_corto,
        info_fichas.titulo_largo
    FROM
        vistas
    LEFT JOIN
        info_fichas ON vistas.codigo = info_fichas.codigo


CREATE TABLE IF NOT EXISTS merged_table(
    codigo TEXT,
    titulo_corto TEXT, 
    titulo_largo TEXT,
    vistas INTEGER
    )

ALTER TABLE fichas ADD COLUMN rubro TEXT;
ALTER TABLE fichas ADD COLUMN subrubro TEXT

UPDATE fichas
SET rubro = ?, subrubro = ?
WHERE codigo = ?
INSERT INTO fichas (rubro, subrubro) VALUES (?, ?)

SELECT codigo, COUNT(*) AS cantidad
FROM fichas
GROUP BY codigo
HAVING COUNT(*) > 1

SELECT * FROM ficha_vistas WHERE titulo_corto IS NOTNULL
