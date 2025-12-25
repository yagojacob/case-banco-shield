import pandas as pd
import os

def carregar_csv(caminho: str, nome_arquivo: str):
    if not os.path.isdir(caminho):
        raise FileNotFoundError('Diretório não encontrado')
    df = pd.read_csv(os.path.join(caminho, f'{nome_arquivo}.csv'))
    return df

def mapeamentos(dim_produto, dim_localidade):
    mapa_produtos = dim_produto.set_index('product_id')['product_name']
    mapa_localidades = dim_localidade.set_index('location_id')['location_name']
    return mapa_produtos, mapa_localidades

#Função para validar as chaves product_id e location_id
def validar_chaves(
        df: pd.DataFrame,
        mapa_produtos: pd.Series,
        mapa_localidades: pd.Series
):

    mascara_valida = (
        df['product_id'].isin(mapa_produtos.index) & df['location_id'].isin(mapa_localidades.index)
    )

    df_valido = df[mascara_valida].copy()
    df_chaves_invalidas = df[~mascara_valida].copy()

    return df_valido, df_chaves_invalidas

#Função para validar a duplicidade de contratos
def validar_duplicidade_contratos(
        df: pd.DataFrame,
        chave='contract_id'
):

    duplicados = df[df.duplicated(subset=[chave], keep=False)]
    df_sem_contratos_duplicados = df.drop_duplicates(subset=[chave], keep=False)

    return duplicados, df_sem_contratos_duplicados

def main():
    caminho = str(input('Digite o caminho do arquivo fatos: '))
    nome_arquivo = str(input('Digite o nome do arquivo fatos (sem .csv): '))
    fatos = carregar_csv(caminho, nome_arquivo)

    caminho = str(input('Digite o caminho do arquivo dim_produtos: '))
    nome_arquivo = str(input('Digite o nome do arquivo dim_produtos (sem .csv): '))
    dim_produto = carregar_csv(caminho, nome_arquivo)

    caminho = str(input('Digite o caminho do arquivo dim_localidade: '))
    nome_arquivo = str(input('Digite o nome do arquivo dim_localidade (sem .csv): '))
    dim_localidade = carregar_csv(caminho, nome_arquivo)

    mapa_produtos, mapa_localidades = mapeamentos(dim_produto, dim_localidade)

    fatos_valido, fatos_chaves_invalidas = validar_chaves(fatos, mapa_produtos, mapa_localidades)
    duplicados, fatos_sem_contratos_duplicados = validar_duplicidade_contratos(fatos, chave='contract_id')

main()