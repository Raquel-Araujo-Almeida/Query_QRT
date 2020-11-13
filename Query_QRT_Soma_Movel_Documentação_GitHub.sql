



/*--------------------------------------------------------------------------------------------------*/
/* Utilizando a variável "NumCons" e a "AvgNumCons": Cálculo 90% + Comentários para documentação    */
/*--------------------------------------------------------------------------------------------------*/

/*

-->Objetivo: Gerar uma base de dados/consulta que contém as informações de "Quantidade de SOs" e o "QRT" baseado no cálculo de uma
		    "Soma Móvel", ou seja, o valor cálculado da linha será referente a soma dos 12 meses, contado com o mês de referência.

--> Principais características: 
	-Período: Contém dados de 2014 até o momento atual (base principal "[SMA_SAS].[dbo].[SMA_Analytics]" utilizada é atualizada diariamente)
	-Granularidade:  Por Ano, Mês, Distribuidora, Categoria e Tipologia 
	-Variáveis disponíveis: IdAgente, Sigla, ClassifAgente, Categoria, Tipologia, AnoCriacao, MesCriacao, QtdeSgo, Numcons e QRT
 

Observações importantes:
	- O método para fazer a soma móvel dos 12 meses utilizados nessa base foi o de somar "as 11 linhas anteriores a linha de referência".
	- Para evitarmos que o cálculo do QRT seja muito afetado por valores errados do Nº de UCs enviado pelas distribuidora,
	   a informação do Número de Unidades Consumidoras utilizado no cálculo do QRT depende da seguinte condição:
			- Se o Nº de UCs for abaixo de 90% da média do Nº de UCs, use a variável "AvgNumCons" 
			- Caso contrário, use a variável "NumCons"

*/


select
	/*Selecionando as variáveis de interesse*/
    L.[IdAgente]
    ,L.[Sigla]
    ,L.[ClassifAgente]
    ,L.[Categoria]
    ,L.[Tipologia]
    ,L.[AnoCriacao]
    ,L.[MesCriacao]
    ,sum(L.Qtde_SO)
		/*Inicio do cálculo da Soma Móvel pela soma das 11 linhas anteriores + a linha de referência =>Criação da variável "QtdeSgo"*/
        OVER(
            PARTITION BY L.[IdAgente], L.[Categoria], L.[Tipologia] /*Particionamento da base por IdAgente, Categoria e Tipologia*/
            ORDER BY L.[IdAgente], L.[Categoria], L.[Tipologia],L.[AnoCriacao],L.[MesCriacao] /*Ordenando por IdAgente, Categoria, Tipologia, Ano e MES */
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW /*Especificação do Particionamento: Linha atual até as 11 linhas anteriores*/
         )  AS QtdeSgo /*Criação da variável "QtdeSgo"*/ 
    ,(
		/*Selecionando o Nº de UCs da base "[SMA_PR_SAMP]"*/
        select iif(NumCons<0.9*AvgNumCons,AvgNumCons,NumCons) /*Condição de seleção do Nº de UCs (Cálculo explicado na especificação da Query)*/
        from [SMA_SAS].[dbo].[SMA_PR_SAMP] as p /*Base que contém a informação de Nº de UCs*/
        where L.Idagente = p.IdAgente and p.Ano = L.[AnoCriacao] and p.Mes = L.[MesCriacao] /*Critério de junção das bases para trazer a variável "Numcons"*/
    ) as Numcons /*Criação da variável "Numcons"*/ 
    ,
    sum(L.Qtde_SO)
		/*Início do cálculo do QRT: Utiliza o valor da soma móvel das 11 linhas+1 (QtdeSgo) 
		   dividido pelo arredondamento do Nº de UCs (Numcons), baseado no cálculo da condição de seleção do Nº de UCs (Cálculo explicado na especificação da Query)   */
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
        ),0,1) * 10000 as QRT /*Criação da variável "QRT"*/

from /* VERIFICAR O PORQUÊ DESSE "FROM" DESSE JEITO: Usando a "agt" e a "cl"*/
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
    FROM   /* VERIFICAR O PORQUÊ DESSE "FROM" DESSE JEITO: Usando a "[SMA_SAS].[dbo].[SMA_Analytics]"*/
        (
            select
                distinct datefromparts(year(Data_Criacao),month(Data_criacao),1) as DataRef /*Montando a variável "DataRef" baseado no ano e no mês da variável "Data_criacao", fixando o dia 1 como referência  */
            from [SMA_SAS].[dbo].[SMA_Analytics]
            where
                
                Data_Criacao between '2014-01-01' and getdate() /*Condição de criar a variável "DataRef": "Data_Criacao" variando entre 2014-01-01 até a data de "hoje" */
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