



/*--------------------------------------------------------------------------------------------------*/
/* Utilizando a vari�vel "NumCons" e a "AvgNumCons": C�lculo 90% + Coment�rios para documenta��o    */
/*--------------------------------------------------------------------------------------------------*/

/*

-->Objetivo: Gerar uma base de dados/consulta que cont�m as informa��es de "Quantidade de SOs" e o "QRT" baseado no c�lculo de uma
		    "Soma M�vel", ou seja, o valor c�lculado da linha ser� referente a soma dos 12 meses, contado com o m�s de refer�ncia.

--> Principais caracter�sticas: 
	-Per�odo: Cont�m dados de 2014 at� o momento atual (base principal "[SMA_SAS].[dbo].[SMA_Analytics]" utilizada � atualizada diariamente)
	-Granularidade:  Por Ano, M�s, Distribuidora, Categoria e Tipologia 
	-Vari�veis dispon�veis: IdAgente, Sigla, ClassifAgente, Categoria, Tipologia, AnoCriacao, MesCriacao, QtdeSgo, Numcons e QRT
 

Observa��es importantes:
	- O m�todo para fazer a soma m�vel dos 12 meses utilizados nessa base foi o de somar "as 11 linhas anteriores a linha de refer�ncia".
	- Para evitarmos que o c�lculo do QRT seja muito afetado por valores errados do N� de UCs enviado pelas distribuidora,
	   a informa��o do N�mero de Unidades Consumidoras utilizado no c�lculo do QRT depende da seguinte condi��o:
			- Se o N� de UCs for abaixo de 90% da m�dia do N� de UCs, use a vari�vel "AvgNumCons" 
			- Caso contr�rio, use a vari�vel "NumCons"

*/


select
	/*Selecionando as vari�veis de interesse*/
    L.[IdAgente]
    ,L.[Sigla]
    ,L.[ClassifAgente]
    ,L.[Categoria]
    ,L.[Tipologia]
    ,L.[AnoCriacao]
    ,L.[MesCriacao]
    ,sum(L.Qtde_SO)
		/*Inicio do c�lculo da Soma M�vel pela soma das 11 linhas anteriores + a linha de refer�ncia =>Cria��o da vari�vel "QtdeSgo"*/
        OVER(
            PARTITION BY L.[IdAgente], L.[Categoria], L.[Tipologia] /*Particionamento da base por IdAgente, Categoria e Tipologia*/
            ORDER BY L.[IdAgente], L.[Categoria], L.[Tipologia],L.[AnoCriacao],L.[MesCriacao] /*Ordenando por IdAgente, Categoria, Tipologia, Ano e MES */
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW /*Especifica��o do Particionamento: Linha atual at� as 11 linhas anteriores*/
         )  AS QtdeSgo /*Cria��o da vari�vel "QtdeSgo"*/ 
    ,(
		/*Selecionando o N� de UCs da base "[SMA_PR_SAMP]"*/
        select iif(NumCons<0.9*AvgNumCons,AvgNumCons,NumCons) /*Condi��o de sele��o do N� de UCs (C�lculo explicado na especifica��o da Query)*/
        from [SMA_SAS].[dbo].[SMA_PR_SAMP] as p /*Base que cont�m a informa��o de N� de UCs*/
        where L.Idagente = p.IdAgente and p.Ano = L.[AnoCriacao] and p.Mes = L.[MesCriacao] /*Crit�rio de jun��o das bases para trazer a vari�vel "Numcons"*/
    ) as Numcons /*Cria��o da vari�vel "Numcons"*/ 
    ,
    sum(L.Qtde_SO)
		/*In�cio do c�lculo do QRT: Utiliza o valor da soma m�vel das 11 linhas+1 (QtdeSgo) 
		   dividido pelo arredondamento do N� de UCs (Numcons), baseado no c�lculo da condi��o de sele��o do N� de UCs (C�lculo explicado na especifica��o da Query)   */
        OVER(
            PARTITION BY L.[IdAgente], L.[Categoria], L.[Tipologia]
            ORDER BY L.[IdAgente], L.[Categoria], L.[Tipologia],L.[AnoCriacao],L.[MesCriacao]
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
         )
    /Round(
        (
        select iif(NumCons<0.9*AvgNumCons,AvgNumCons,NumCons)
        from [SMA_SAS].[dbo].[SMA_PR_SAMP] as p
        where L.Idagente = p.IdAgente
        and p.Ano = L.[AnoCriacao] and p.Mes = L.[MesCriacao]
        ),0,1) * 10000 as QRT /*Cria��o da vari�vel "QRT"*/

from /* VERIFICAR O PORQU� DESSE "FROM" DESSE JEITO: Usando a "agt" e a "cl"*/
(           
    SELECT
        agt.[IdAgente_SMA] as IdAgente
        ,agt.[Sigla_SMA] as Sigla
        ,agt.[ClassifAgente]
        ,cl.[Categoria]
        ,cl.[Tipologia]
        ,year(tp.DataRef) As AnoCriacao
        ,month(tp.DataRef) AS MesCriacao
        ,count ([Numero_da_Solicitacao]) as Qtde_SO
    FROM   /* VERIFICAR O PORQU� DESSE "FROM" DESSE JEITO: Usando a "[SMA_SAS].[dbo].[SMA_Analytics]"*/
        (
            select
                distinct datefromparts(year(Data_Criacao),month(Data_criacao),1) as DataRef /*Montando a vari�vel "DataRef" baseado no ano e no m�s da vari�vel "Data_criacao", fixando o dia 1 como refer�ncia  */
            from [SMA_SAS].[dbo].[SMA_Analytics]
            where
                
                Data_Criacao between '2014-01-01' and getdate() /*Condi��o de criar a vari�vel "DataRef": "Data_Criacao" variando entre 2014-01-01 at� a data de "hoje" */
        ) AS tp
        cross join [SMA_SAS].[dbo].[SMA01_IdAgente] as agt
        cross join (select distinct codigo_sequencial, Categoria, Tipologia  from [SMA_SAS].[dbo].[SMA_Analytics]) as cl
        left join  [SMA_SAS].[dbo].[SMA_Analytics] as at on at.IdAgente=agt.IdAgente_SMA and at.codigo_sequencial=cl.codigo_sequencial and at.AnoCriacao=year(tp.DataRef) and at.MesCriacao=month(tp.DataRef)
    --where agt.IdAgente_SMA not in ('31')
	--where agt.IdAgente_SMA  in ('38') and AnoCriacao in ('2016') and MesCriacao in ('2')
    GROUP BY
        agt.[IdAgente_SMA]
        ,agt.[Sigla_SMA]
        ,agt.[ClassifAgente]
        ,cl.[Categoria]
        ,cl.[Tipologia]
        ,tp.DataRef
) as L
ORDER BY
    L.[IdAgente]
    ,L.[Sigla]
    ,L.[ClassifAgente]
    ,L.[Categoria]
    ,L.[Tipologia]
    ,L.[AnoCriacao]
    ,L.[MesCriacao]