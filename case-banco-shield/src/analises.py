import tratamento_dos_dados
import pandas as pd
import numpy as np

def data_frames(dados):
    dim_produto_processed = dados['dim_produto_processed']
    dim_localidade_processed = dados['dim_localidade_processed']
    fato_contratos_processed = dados['fato_contratos_processed']
    return dim_produto_processed, dim_localidade_processed, fato_contratos_processed

def mapeamentos(dim_produto_processed, dim_localidade_processed):
    mapa_produtos = dim_produto_processed.set_index('product_id')['product_name']
    mapa_categorias = dim_produto_processed.set_index('product_id')['category']
    mapa_localidades = dim_localidade_processed.set_index('location_id')['location_name']
    mapa_macrorregioes = dim_localidade_processed.set_index('location_id')['macro_region']
    return mapa_produtos, mapa_categorias, mapa_localidades, mapa_macrorregioes

def produtos_mais_vendidos_quantidade(fato_contratos_processed, mapa_produtos, mapa_categorias, banco='Banco Shield'):

    try:
        #IDs dos produtos em ordem decrescente de quantidades
        produtos_ids = (
            fato_contratos_processed.loc[fato_contratos_processed['bank'] == banco, 'product_id']
            .value_counts()
        )
        
        #Produto mais vendido (primeiro da lista)
        produto_mais_vendido_id = produtos_ids.index[0]
        produto_mais_vendido_nome = mapa_produtos.get(produto_mais_vendido_id)
        categoria = mapa_categorias.get(produto_mais_vendido_id)
        quantidade = produtos_ids.iloc[0]
    
    except IndexError:
        print(f'Nenhum produto encontrado para o banco {banco}. Possível erro de digitação.')
        produto_mais_vendido_id = None
        produto_mais_vendido_nome = None
        categoria = None
        quantidade = 0
    
    #Impressão dos resultados
    print(f'Banco: {banco}.')
    print(f'Produto mais vendido (ID): {produto_mais_vendido_id}.')
    print(f'Produto mais vendido (nome): {produto_mais_vendido_nome}.')
    print(f'Categoria: {categoria}.')
    print(f'Quantidade de contratos: {quantidade}.')

def produtos_mais_vendidos_valor(fato_contratos_processed, mapa_produtos, mapa_categorias, banco='Banco Shield'):

    try:
        #IDs dos produtos em ordem decrescente de valores
        produtos_ids = (
            fato_contratos_processed.loc[fato_contratos_processed['bank'] == banco] #Pesquisa somenete para o banco dado
            .groupby('product_id')['financed_amount'] #“Para cada product_id, considere apenas a coluna financed_amount”
            .sum() #Soma os valores
           .sort_values(ascending=False) #Coloca em ordem decrescente
        )

        #Produto mais vendido (primeiro da lista)
        produto_mais_vendido_id = produtos_ids.index[0]
        produto_mais_vendido_nome = mapa_produtos.get(produto_mais_vendido_id)
        categoria = mapa_categorias.get(produto_mais_vendido_id)
        valor_total = produtos_ids.iloc[0]
    
    except IndexError:
        print(f'Nenhum produto encontrado para o banco {banco}. Possível erro de digitação.')
        produto_mais_vendido_id = None
        produto_mais_vendido_nome = None
        categoria = None
        valor_total = 0

    #Impressão dos resultados
    print(f'Banco: {banco}.')
    print(f'Produto mais vendido (ID): {produto_mais_vendido_id}.')
    print(f'Produto mais vendido (nome): {produto_mais_vendido_nome}.')
    print(f'Categoria: {categoria}.')
    print(f'Valor (R$): {valor_total}.')

def localidades_mais_fortes(fato_contratos_processed, mapa_localidades, mapa_macrorregioes, banco='Banco Shield'):

    try:
        localidades_ids = (
            fato_contratos_processed.loc[fato_contratos_processed['bank'] == banco] #Pesquisa somenete para o banco dado
            .groupby('location_id')['financed_amount'] #“Para cada location_id, considere apenas a coluna financed_amount”
            .sum() #Soma os valores
           .sort_values(ascending=False) #Coloca em ordem decrescente
        )

        #Localidade mais forte (primeira da lista)
        localidade_mais_forte_id = localidades_ids.index[0]
        localidade_mais_forte_nome = mapa_localidades.get(localidade_mais_forte_id)
        localidade_mais_forte_macrorregiao = mapa_macrorregioes.get(localidade_mais_forte_id)
        valor_total = localidades_ids.iloc[0]
        total_geral = localidades_ids.sum()
        porcentagem = (valor_total/total_geral * 100) if total_geral > 0 else 0
    
    except IndexError:
        print(f'Nenhuma localidade encontrada para o banco {banco}. Possível erro de digitação.')
        localidade_mais_forte_id = None
        localidade_mais_forte_nome = None
        localidade_mais_forte_macrorregiao = None
        valor_total = 0
        total_geral = 0
        porcentagem = 0
    
    #Impressão dos resultados
    print(f'Banco: {banco}.')
    print(f'Localidade mais forte (ID): {localidade_mais_forte_id}.')
    print(f'Localidade mais forte (nome): {localidade_mais_forte_nome}.')
    print(f'Macro-região: {localidade_mais_forte_macrorregiao}.')
    print(f'Valor (R$): {valor_total} ({porcentagem:.2f}%).')

def maior_risco_inadimplencia_produto(fato_contratos_processed, mapa_produtos, mapa_categorias, banco='Banco Shield'):

    risco_produtos = (
        fato_contratos_processed
        .loc[fato_contratos_processed['bank'] == banco] #Pesquisa somenete para o banco dado
        .groupby('product_id') #Agrupa por produtos
        .agg( #Inicia agregações múltiplas; 'para cada product_id calcule as métricas...'
            taxa_30p=('dpd', lambda x: (x >= 30).mean()), #Proporção de contratos com dod >= 30; média de booleano
            volume_contratos=('contract_id', 'count'), #Calcula quantidade de contratos
            valor_total=('financed_amount', 'sum'), #Valor de todos os contratos
            valor_em_risco=('delinquent_amount_30p', 'sum') #Valor em risco dos contratos com dpd >= 30
        )
    )

    risco_produtos['product_name'] = risco_produtos.index.map(mapa_produtos).fillna('Produto não especificado') #Mapenado os nomes dos produtos
    risco_produtos['category'] = risco_produtos.index.map(mapa_categorias).fillna('Categoria não especificada') #Mapeando as categoria dos produtos
    risco_produtos['taxa_30p'] = (risco_produtos['taxa_30p'] * 100).round(2) #Assume a taxa30p em porcentagem
    risco_produtos = risco_produtos.sort_values('valor_em_risco', ascending=False) #Ordena em ordem decrescente conforme score de rsico
    risco_produtos = risco_produtos[['product_name', 'category', 'taxa_30p', 'volume_contratos', 'valor_total', 'valor_em_risco']]

    risco_produtos.to_csv( #Será útil para os dashboards
    f'C:/YAGO/case-banco-shield/data/processed/risco_produtos_{banco}.csv',
    index=False,
    sep=',',          #Separador
    encoding='utf-8', #Codificação dos caracteres
    decimal='.',      #Separador decimal
    )

    print(f'Banco: {banco}.')
    print(f'Produtos em ordem decrescente de risco:\n{risco_produtos.to_string()}')

def maior_risco_inadimplencia_localidade_estatistico(fato_contratos_processed, mapa_localidades, mapa_macrorregioes, banco='Banco Shield'):
    
    risco_localidades =(
        fato_contratos_processed
        .loc[fato_contratos_processed['bank'] == banco] #Pesquisa somenete para o banco dado
        .groupby('location_id') #Agrupa por localidades
        .agg( #Inicia agregações múltiplas; 'para cada location_id calcule as métricas...'
            taxa_30p=('dpd', lambda x: (x >= 30).mean()), #Proporção de contratos com dod >= 30; média de booleano
            volume_contratos=('contract_id', 'count'), #Calcula quantidade de contratos
            valor_total=('financed_amount', 'sum'), #Valor de todos os contratos
            valor_em_risco=('delinquent_amount_30p', 'sum') #Valor dos contratos com dpd >= 30
        )
    )

    risco_localidades['location_name'] = risco_localidades.index.map(mapa_localidades).fillna('Localidade não especificada') #Mapenado os nomes das localidades
    risco_localidades['macro_region'] = risco_localidades.index.map(mapa_macrorregioes).fillna('Macrorregião não especificada') #Mapeando as macrorregiões
    risco_localidades = risco_localidades.sort_values('valor_em_risco', ascending=False) #Ordena em ordem decrescente conforme score de rsico
    risco_localidades['taxa_30p'] = (risco_localidades['taxa_30p'] * 100).round(2) #Assume a taxa30p em porcentagem
    risco_localidades = risco_localidades[['location_name', 'macro_region', 'taxa_30p', 'volume_contratos', 'valor_total', 'valor_em_risco']]

    risco_localidades.to_csv( #Será útil para os dashboards
    f'C:/YAGO/case-banco-shield/data/processed/risco_localidades_{banco}_e.csv',
    index=False,
    sep=';',          #Separador
    encoding='utf-8', #Codificação dos caracteres
    decimal=',',      #Separador decimal
    )

    print(f'Banco: {banco}.')
    print(f'Localidades em ordem decrescente de risco (estatístico):\n{risco_localidades.to_string()}')

def maior_risco_inadimplencia_localidade_heuristico(fato_contratos_processed, dim_localidade_processed, mapa_localidades, mapa_macrorregioes, banco='Banco Shield'):
    
    #Exposição financeira por localidade
    explosicao_localidades = (
        fato_contratos_processed
        .loc[fato_contratos_processed['bank'] == banco] #Pesquisa somenete para o banco dado
        .groupby('location_id') #Agrupa por localidades...
        .agg( #... e realiza agregações múltiplas
            valor_total=('financed_amount', 'sum'),
            volume_de_contratos=('contract_id', 'count')
        )
    )
    
    #risk_factor por localidade vindo da dim_localidade_processed
    risk_factor = (
        dim_localidade_processed
        .set_index('location_id')['risk_factor_region']
    )

    #risk_factor_region
    risco_localidades = explosicao_localidades.copy()

    risco_localidades['risk_factor_region'] = (
        risco_localidades.index.map(risk_factor)
    )

    #Mapeando localidades e macroregiões
    risco_localidades['location_name'] = risco_localidades.index.map(mapa_localidades).fillna('Localidade não especificada')
    risco_localidades['macro_region'] = risco_localidades.index.map(mapa_macrorregioes).fillna('Macrorregião não especificada')

    #Ordem crescente
    risco_localidades = risco_localidades.sort_values(
        'risk_factor_region',
        ascending=False
    )

    #Seleção final
    risco_localidades = risco_localidades[['location_name', 'macro_region', 'volume_de_contratos', 'valor_total', 'risk_factor_region']]

    print(f'Banco: {banco}.')
    print(f'Localidades em ordem decrescente de risco (heurístico):\n{risco_localidades.to_string()}')

def main():
    dados = tratamento_dos_dados.data(caminho='C:/YAGO/case-banco-shield/data/processed', processed=True) #Abrindo os arquivos já tratados com as funções desenvolvidas
    dim_produto_processed, dim_localidade_processed, fato_contratos_processed = data_frames(dados)
    mapa_produtos, mapa_categorias, mapa_localidades, mapa_macrorregioes = mapeamentos(dim_produto_processed, dim_localidade_processed)

    #Questão 1.
    produtos_mais_vendidos_quantidade(fato_contratos_processed, mapa_produtos, mapa_categorias)
    produtos_mais_vendidos_valor(fato_contratos_processed, mapa_produtos, mapa_categorias)
    produtos_mais_vendidos_quantidade(fato_contratos_processed, mapa_produtos, mapa_categorias, banco='Hidra')
    produtos_mais_vendidos_valor(fato_contratos_processed, mapa_produtos, mapa_categorias, banco='Hidra')
    print('\n\n')

    #Questão 2.
    localidades_mais_fortes(fato_contratos_processed, mapa_localidades, mapa_macrorregioes)
    localidades_mais_fortes(fato_contratos_processed, mapa_localidades, mapa_macrorregioes, banco='Hidra')
    print('\n\n')

    # #Questão 3
    maior_risco_inadimplencia_produto(fato_contratos_processed, mapa_produtos, mapa_categorias)
    maior_risco_inadimplencia_localidade_estatistico(fato_contratos_processed, mapa_localidades, mapa_macrorregioes)
    maior_risco_inadimplencia_localidade_heuristico(fato_contratos_processed, dim_localidade_processed, mapa_localidades, mapa_macrorregioes)
    maior_risco_inadimplencia_produto(fato_contratos_processed, mapa_produtos, mapa_categorias, banco='Hidra')
    maior_risco_inadimplencia_localidade_estatistico(fato_contratos_processed, mapa_localidades, mapa_macrorregioes, banco='Hidra')
    maior_risco_inadimplencia_localidade_heuristico(fato_contratos_processed, dim_localidade_processed, mapa_localidades, mapa_macrorregioes, banco='Hidra')

main()