"""
Jose Paulo
Teste2_Transformar_Dados
"""
from tabula import read_pdf
import pandas as pd
from os import mkdir

# NOME_PDF = "../teste1_webscrapping/padrao-tiss_componente-organizacional_202111.pdf"
NOME_PDF = "padrao-tiss_componente-organizacional_202111.pdf"


def ler_pdf(pagina="all", area=None, multiple=False) -> pd.DataFrame:
    """
    Lê um pdf e retorna um dataframe com os dados
    :param pagina: página(s) a ser(em) lida(s)
    :param area: área a ser lida
    :param multiple: se é para ler múltiplas páginas
    :return: dataframe com os dados da(s) pagina(s) do pdf
    """
    df = read_pdf(
        "./" + NOME_PDF,
        pages=pagina,
        encoding="utf-8",
        area=area,
        multiple_tables=multiple,
    )
    return df


def criar_dataframe_tabela_unica(dados: pd.DataFrame, nome_tabela: str) -> pd.DataFrame:
    """
    Criar um dataframe com os dados do quadro
    :param dados: dataframe com os dados do quadro
    :param nome_tabela: nome da tabela do quadro a ser transformada
    :return: dataframe com os dados do quadro transformados
    """

    # O primeiro elemento da tabela neste caso, é uma string que vai representar
    # as colunas da tabela, essa string é dividida de forma a identificar a label da coluna.
    # 'Código,Descrição da categoria' -> col_1='Código', col_2='Descrição da categoria'
    col_1 = dados[nome_tabela][0].split(",")[0]
    col_2 = dados[nome_tabela][0].split(",")[1]
    df = pd.DataFrame(columns=[col_1, col_2])

    # Percorre os dados da tabela
    for linha in range(1, len(dados), 1):
        pos = 0
        str_convertida = ""

        # As strings não tem separação definida, então enquanto não achar um caracter vazio,
        # vai concatenando a string de forma a obter o código. Logo,
        # '1 Exemplo de categoria' -> '1'
        while dados[nome_tabela][linha][pos] != " ":
            str_convertida += dados[nome_tabela][linha][pos]
            pos += 1

        # A string convertida é usada na col_1, enquanto a col_2 pega o restante da string
        # a partir da posição+1 em que o caracter vazio foi encontrado
        # '1 Exemplo de categoria' -> 'Exemplo de categoria'
        df = df.append(
            {col_1: str_convertida, col_2: dados[nome_tabela][linha][pos + 1 :]},
            ignore_index=True,
        )

    return df


def transformar_quadro_multiplo(dados: pd.DataFrame, nome_tabela: str) -> pd.DataFrame:
    """
    Transforma quadros com multiplas tabelas
    As tabelas devem ter o formato de duas colunas -> [Código, Descrição da categoria]
    :param dados: dataframe com os dados do quadro
    :param nome_tabela: nome da tabela
    :return: dataframe com os dados do quadro transformados
    """

    # Usado para identificar a label da coluna da primeira tabela dos dados
    col_tab_1 = "Unnamed: 0"  # Essa string representa a primeira coluna da tabela
    col_tab_2 = nome_tabela  # Essa string representa a segunda coluna da tabela

    # A partir das identificações acima, é criado um dataframe final
    # utilizando como colunas, o primeiro elemento da tabela em questão
    # col_df_1 = "Código" -> col_df_2 = "Descrição da categoria"
    col_df_1 = dados[0][col_tab_1].pop(0)
    col_df_2 = dados[0][col_tab_2].pop(0)
    df = pd.DataFrame(columns=[col_df_1, col_df_2])

    inicio = 1

    # Percorre todas as tabelas de cada pagina escolhida do pdf
    for i in range(len(dados) - 1):

        # Caso não for a primeira tabela
        if i > 0:

            # As colunas dessa tabela deveriam ser um elemento do dataframe final
            # então, elas são atribuidas a uma variavel
            row = pd.Series(dados[i].columns, index=dados[i].columns)

            # Essa informção também é a identificação da coluna da tabela atual
            # então, vai ser utilizada como label para percorrer os dados da tabela atual
            col_tab_1 = row[0]
            col_tab_2 = row[1]

            # É adicionado ao dataframe final o a linha que não deveria ser coluna
            # de forma a dar sequencia nos dados
            df = df.append(
                {col_df_1: col_tab_1, col_df_2: col_tab_2}, ignore_index=True
            )

            inicio = 0

        # Percorre todas as linhas da tabela atual
        for linha in range(inicio, len(dados[i]), 1):
            # Adiciona a linha atual ao dataframe final
            df = df.append(
                {
                    col_df_1: dados[i][col_tab_1][linha],
                    col_df_2: dados[i][col_tab_2][linha],
                },
                ignore_index=True,
            )

    # Converte o dataframe final para CSV
    df.to_csv(
        "./quadros_extraidos/" + nome_tabela.replace(" ", "_") + ".csv",
        index=False,
        encoding="utf-8",
    )


def transformar_quadro_unico(dados: pd.DataFrame, nome_tabela: str) -> None:
    """
    Transforma um quadro com uma única tabela
    A tabela devem ter o formato de duas colunas -> [Código, Descrição da categoria]
    :param dados: dataframe com os dados do quadro
    :param nome_tabela: nome da tabela do quadro a ser transformada
    :return: None
    """

    # Neste caso, o primeiro tabela lida é o unica do quadro
    dados = dados[0]

    # Transforma a primeira linha da tabela em uma string que vai representar
    # as colunas da tabela, é dividida por vírgula de forma a identificar a label da coluna.
    dados[nome_tabela] = dados[nome_tabela].replace(
        "Código Descrição da categoria", "Código,Descrição da categoria"
    )

    df = criar_dataframe_tabela_unica(dados, nome_tabela)

    df.to_csv(
        "./quadros_extraidos/" + nome_tabela.replace(" ", "_") + ".csv",
        index=False,
        encoding="utf-8",
    )


def criar_pasta_quadros() -> None:
    """
    Cria a pasta para os quadros extraidos
    :return: None
    """
    try:
        mkdir("./quadros_extraidos")
    except FileExistsError:
        pass


def main() -> None:
    """
    Função principal
    :return: None
    """

    criar_pasta_quadros()

    # Quadro 30
    dados = ler_pdf(pagina="114")
    transformar_quadro_unico(dados, "Tabela de Tipo do Demandante")

    # Quadro 31
    dados = ler_pdf(pagina="115-120", multiple=True)
    transformar_quadro_multiplo(dados, "Tabela de Categoria do Padrão TISS")

    # Quadro 32
    dados = ler_pdf(pagina="120", area=(442, 132, 442 + 69, 132 + 180))
    transformar_quadro_unico(dados, "Tabela de Tipo de Solicitação")


__name__ == "__main__" and main()
