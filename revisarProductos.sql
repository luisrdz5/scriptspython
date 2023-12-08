SELECT "id", (SELECT name FROM public."categories" WHERE id=A.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Arneses" A WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=B.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Bocinas" B WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=C.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Estereos" C WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=D.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Woofers" D WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=E.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Cajones" E WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=F.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Bases" F WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=G.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Amplificadores" G WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=H.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Frentes" H WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=I.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Tweeters" I WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=J.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Ecualizadores" J WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=K.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Epicentros" K WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=L.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Procesadores" L WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=M.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."KitsCables" M WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=N.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Accesorios" N WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=O.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."AdaptadoresImpedancia" O WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=P.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."MediosRangos" P WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=Q.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."basesBocina" Q WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=R.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."AdaptadoresAntena" R WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=S.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."audioMarino" S WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=T.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."iluminacion" T WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=U.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."accesoriosCamioneta" U WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=V.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."servicios" V WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=W.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."video" W WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=X.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."seguridad" X WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=Y.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Componentes" Y WHERE "fotosId" IS NULL
UNION
SELECT "id", (SELECT name FROM public."categories" WHERE id=Z.category_id) as categoria, "Categoria" as "categoriaEnTabladeProducto", "Modelo", "sku", "fotosId" FROM public."Amplificadores3en1" Z WHERE "fotosId" IS NULL;