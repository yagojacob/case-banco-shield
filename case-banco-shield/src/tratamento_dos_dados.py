import pandas as pd
import os

def opening_files(caminho, processed=False): #Função para inicializar os arquivos csv
    
    #Definindo o caminho em que os arquivos csv deverão ser procurados
    if not os.path.isdir(caminho):
        raise FileNotFoundError('Diretório não encontrado')
    
    #Definindo o sufixo '_processed' para lidar com os eventuais arquivos processados
    sufixo = '_processed' if processed else '' 

    #Abrindo os arquivos
    dim_produto = pd.read_csv(os.path.join(caminho, f'dim_produto{sufixo}.csv'))
    dim_localidade = pd.read_csv(os.path.join(caminho, f'dim_localidade{sufixo}.csv'))
    fato_contratos = pd.read_csv(os.path.join(caminho, f'fato_contratos{sufixo}.csv'))

    return dim_produto, dim_localidade, fato_contratos

def data(caminho, processed=False):
    
    #Abrindo os arquivos
    dim_produto, dim_localidade, fato_contratos = opening_files(caminho, processed)
    
    #Definindo o sufixo '_processed' para lidar com os eventuais arquivos processados
    sufixo = '_processed' if processed else '' 
    
    dados = {
        f'dim_produto{sufixo}': dim_produto,
        f'dim_localidade{sufixo}': dim_localidade,
        f'fato_contratos{sufixo}': fato_contratos,
    }

    return dados

def tratamento_dos_dados(dados): #Função para o tratamento dos dados
    
    #Criando e salvando cópias seguras para dim_produto e dim_localidade (não apresentam ruídos)
    dim_produto_processed = dados['dim_produto'].copy(deep=True)
    dim_produto_processed.to_csv('C:/YAGO/case-banco-shield/data/processed/dim_produto_processed.csv')
    dim_localidade_processed = dados['dim_localidade'].copy(deep=True)
    dim_localidade_processed.to_csv('C:/YAGO/case-banco-shield/data/processed/dim_localidade_processed.csv')
    
    #Criando uma cópia segura para edição do arquivo fato_contratos (apresenta ruídos)
    fato_contratos_processed = dados['fato_contratos'].copy(deep=True) #Criando uma cópia segura para edição do arquivo fato_contratos

    #Coluna contract_id
    contratos_validos = fato_contratos_processed['contract_id'].value_counts()
    contratos_validos = contratos_validos[contratos_validos == 1].index.tolist()
    fato_contratos_processed = fato_contratos_processed[fato_contratos_processed['contract_id'].isin(contratos_validos)]

    #Coluna ano_mes
    periodos_validos = [i for i in range(202501, 202513)] #De 202501 a 202512
    fato_contratos_processed = fato_contratos_processed[fato_contratos_processed['ano_mes'].isin(periodos_validos)]

    #Coluna bank
    bancos_validos = ['Banco Shield', 'Hidra']
    fato_contratos_processed = fato_contratos_processed[fato_contratos_processed['bank'].isin(bancos_validos)]

    #Colunas product_id e location_id
    mapa_produtos = dim_produto_processed.set_index('product_id')['product_name'] #Mapeamento dos produtos por ID
    produtos_validos = mapa_produtos.index
    mapa_localidades = dim_localidade_processed.set_index('location_id')['location_name'] #Mapeamento das localidades por ID
    localidades_validas = mapa_localidades.index
    fato_contratos_processed = fato_contratos_processed[fato_contratos_processed['product_id'].isin(produtos_validos) & fato_contratos_processed['location_id'].isin(localidades_validas)]

    #Coluna financed_amount
    fato_contratos_processed = fato_contratos_processed[fato_contratos_processed['financed_amount'].ge(0)]

    #Coluna outstanding_balance
    fato_contratos_processed = fato_contratos_processed[fato_contratos_processed['outstanding_balance'].ge(0)]

    #Coluna delinquent_amount_30p
    fato_contratos_processed.loc[fato_contratos_processed['dpd'] < 30, 'delinquent_amount_30p'] = 0

    #Coluna risk_score
    fato_contratos_processed = fato_contratos_processed[fato_contratos_processed['risk_score'].between(0, 1)]

    #Salvando a cópia após o tratamento dos dados
    fato_contratos_processed.to_csv(
    'C:/YAGO/case-banco-shield/data/processed/fato_contratos_processed.csv',
    index=False,
    sep=',',          #Separador
    encoding='utf-8', #Codificação dos caracteres
    decimal='.',      #Separador decimal
    )

def main():
    dados = data('C:/YAGO/case-banco-shield/data/raw', processed=False)
    tratamento_dos_dados(dados)

main()