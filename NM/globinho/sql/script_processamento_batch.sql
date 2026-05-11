

UPDATE TempInternationalTrade
SET UNID  = isnull(UNID, '')
   , VIA   = isnull(VIA, '')
   , UF   = isnull(UF, '')
   , Quantidade = isnull(Quantidade, 0)
   , Frete  = isnull(Frete, 0)
   , Seguro  = isnull(Seguro, 0)
   , ValorCIF = isnull(ValorCIF, 0)
--------------------------------
/* Carregar índices principais */
--------------------------------
DROP TABLE IF EXISTS #IndicePrincipal;

CREATE TABLE #IndicePrincipal (
   IDIndicePrincipal INT
      , NCMID    INT
      , PaisID    INT
      , IDNCMPais   INT
      , ImportExport  TINYINT
      , UNIQUE (IDIndicePrincipal, NCMID, PaisID, IDNCMPais, ImportExport)
      )

      INSERT INTO #IndicePrincipal ( IDIndicePrincipal, NCMID, PaisID, IDNCMPais, ImportExport)
   SELECT IDIndicePrincipal
   , NCMID
   , PaisID
   , (SELECT TOP 1 IDNCMPais FROM tblNCM_Pais NCP WHERE NCP.PaisID = p.PaisID) AS IDNCMPais
   , ImportExport
   FROM tblIndicePrincipal p
   WHERE IDIndicePrincipal IN (SELECT DISTINCT IDIndicador FROM TempInternationalTrade);

CREATE NONCLUSTERED INDEX sk01_#IndicePrincipal ON #IndicePrincipal(IDIndicePrincipal)

drop table if exists #tmp_insert_tblSecexArquivo_CIF;

;with duplicidade as (
      SELECT p.NCMID, t.IDIndicador, t.DataNCM, t.UNID, p.ImportExport
   , t.IDPais, t.UF, t.VIA, t.Quantidade, t.Peso
   , t.FOB, t.Frete, t.Seguro, t.ValorCIF, p.IDNCMPais
, ROW_NUMBER() OVER (PARTITION BY t.IDIndicador, p.NCMID, t.DataNCM,  t.UNID, p.ImportExport
   , t.UF, t.VIA, p.IDNCMPais  ORDER BY t.IDIndicador ASC) as rn 
   FROM TempInternationalTrade t
   JOIN #IndicePrincipal   p ON p.IDIndicePrincipal = t.IDIndicador
   -- Esse join verifica se a chave já existe
   LEFT JOIN cmx.tblSecexArquivo_CIF s ON s.NCMID  = p.NCMID
            AND s.PaisID  = t.IDPais
            AND s.DataNCM  = t.DataNCM
            AND s.UNID   = t.UNID
            AND s.IE   = p.ImportExport
            AND s.UF   = t.UF
            AND s.VIA   = t.VIA
            AND s.PaisIDOrigem = p.IDNCMPais
   WHERE s.NCMID IS NULL
--   and p.NCMID = 176710
--   AND t.IDPais = 23
--AND t.DataNCM = '2020-11-01'
)
select *
into #tmp_insert_tblSecexArquivo_CIF
from duplicidade 
where rn = 1


----------------------------------------------
/* Inserir na tabela cmx.tblSecexArquivo_CIF */
----------------------------------------------
      INSERT INTO cmx.tblSecexArquivo_CIF
   ( NCMID, IndicePrincipalID, DataNCM, UNID, IE
   , PAIS, UF, VIA, Quantidade, Peso
   , FOB, Frete, Seguro, CIF, PaisIDOrigem )
   SELECT NCMID, IDIndicador, DataNCM, UNID, ImportExport
   , IDPais, UF, VIA, Quantidade, Peso
   , FOB, Frete, Seguro, ValorCIF, IDNCMPais
from #tmp_insert_tblSecexArquivo_CIF



-------------------------------------------
/* Criar tabela temporária com agregações */
-------------------------------------------
DROP TABLE IF EXISTS #tblSecexArquivo_CIFTemp;

         SELECT
               saCIF.NCMID    AS NCMID,
               saCIF.IndicePrincipalID AS IndicePrincipalID,
               NCP_Pais.PaisID   AS PaisID,
               saCIF.IE    AS ImportExport,
               saCIF.DataNCM   AS DataNCM,
               SUM(saCIF.Peso)   AS ValorPeso,
               SUM(saCIF.FOB)   AS ValorFOB,
               IIF(
                  SUM(saCIF.Peso) > 0, 
                  (SUM(saCIF.quantidade) + SUM(saCIF.frete) + SUM(saCIF.seguro)) / SUM(saCIF.Peso),
                  0
               )      AS ValorCIF,
               SUM(saCIF.Quantidade) AS Quantidade,
               NCP_Origem.PaisID  AS PaisIDOrigem
         INTO #tblSecexArquivo_CIFTemp
         FROM CMX.tblSecexArquivo_CIF saCIF
JOIN TempInternationalTrade t ON saCIF.DataNCM = t.DataNCM
         AND saCIF.PAISID = t.IDPais
JOIN #IndicePrincipal   p ON t.IDIndicador = p.IDIndicePrincipal
         AND saCIF.NCMID = p.NCMID
         AND saCIF.IE = p.ImportExport
         AND saCIF.PaisIDOrigem = p.IDNCMPais
   LEFT JOIN tblNCM_Pais    NCP_Pais   ON NCP_Pais.IDNCMPais   = saCIF.PAISID
   LEFT JOIN tblNCM_Pais    NCP_Origem ON NCP_Origem.IDNCMPais = saCIF.PaisIDOrigem
GROUP BY saCIF.NCMID, saCIF.IndicePrincipalID, NCP_Pais.PaisID
   , saCIF.IE, saCIF.DataNCM, NCP_Origem.PaisID;


-----------------------------------------
/* Cria tabela com valores consolidados */
-----------------------------------------
DROP TABLE IF EXISTS #tblSecexArquivo_CIF_Consolidado;

SELECT NCMID, IndicePrincipalID, PaisID
   , ImportExport, DataNCM, SUM(ValorPeso) AS ValorPeso
   , SUM(ValorFOB) AS ValorFOB, SUM(ValorCIF) AS ValorCIF, SUM(Quantidade) AS Quantidade
   , PaisIDOrigem
   INTO #tblSecexArquivo_CIF_Consolidado
   FROM #tblSecexArquivo_CIFTemp
GROUP BY NCMID, IndicePrincipalID, PaisID
   , ImportExport, DataNCM, PaisIDOrigem

---------------------------------------------------------
/* UPDATE - Dados que já existem na cmx.tblSecexArquivo */
---------------------------------------------------------
UPDATE B 
   SET B.ValorPeso         = A.ValorPeso
   , B.ValorFOB          = A.ValorFOB
   , B.ValorCIF          = A.ValorCIF
   , B.Quantidade        = A.Quantidade
   , B.PaisIDOrigem      = A.PaisIDOrigem
   , B.IndicePrincipalID = A.IndicePrincipalID
   FROM #tblSecexArquivo_CIF_Consolidado A
   JOIN cmx.tblSecexArquivo    B on B.NCMID  = A.NCMID
            AND B.PaisID  = A.PaisID
            AND B.ImportExport = A.ImportExport
            AND B.DataNCM  = A.DataNCM
            AND B.PaisIDOrigem = A.PaisIDOrigem

----------------------------------------------------------
/* INSERT - Dados que NÃO existem na cmx.tblSecexArquivo */
----------------------------------------------------------
INSERT INTO cmx.tblSecexArquivo 
   ( NCMID, IndicePrincipalID, PaisID, ImportExport, DataNCM
   , ValorPeso, ValorFOB, ValorCIF, Quantidade, PaisIDOrigem 
   )
   SELECT A.NCMID, A.IndicePrincipalID, A.PaisID, A.ImportExport, A.DataNCM
   , A.ValorPeso, A.ValorFOB, A.ValorCIF, A.Quantidade, A.PaisIDOrigem
   FROM #tblSecexArquivo_CIF_Consolidado A
LEFT JOIN cmx.tblSecexArquivo     B on B.NCMID  = A.NCMID
            AND B.PaisID  = A.PaisID
            AND B.ImportExport = A.ImportExport
            AND B.DataNCM  = A.DataNCM
            AND B.PaisIDOrigem = A.PaisIDOrigem
   WHERE B.NCMID IS NULL

TRUNCATE TABLE TempInternationalTrade;
DROP TABLE #TblSecexArquivo_CIFTemp;