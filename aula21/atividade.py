# Atividade
# O Secretário de estado da Polícia Militar, ligou pra você e pediu que o auxiliasse em um estudo sobre os Roubos de Cargas registrados nos Batalhões de Polícia Militar, entre os anos de 2022 e 2023.
# Ele te pediu o seguinte:
# Verificar se existem Batalhões de Polícia Militar que estão registrando muitos Roubos de Cargas, além do que pode ser considerado normal, dado os registros dos outros BPMs
# Verificar se há um padrão de registros de ocorrências, de modo que seja possível definir uma média de Roubos de Cargas através do BPMs, para ser informada a mídia
# Gostaria que me enviasse isso de forma gráfica:
# Se houver BPMs com registros de Roubos de Cargas muito acima dos demais, exiba o ranking desses BPMs. Caso não haja, exiba no ranking de todos os BPMs, através dos registros de Roubos de Cargas
# Exiba também métricas que possam sustentar o argumento de que a média é uma medida confiável para ser utilizada e que o conjunto de dados possui um determinado padrão de registros dessas ocorrências

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

host = 'localhost'
user = 'root'
password = '0933'
database = 'bd_roubos_cargas'


try:
    engine = create_engine(
        f'mysql+pymysql://{user}:{password}@{host}/{database}'
    )

    query = (
        'SELECT b.ano, b.munic, b.cod_ocorrencia, r.roubo_carga '
        'FROM basedp b '
        'JOIN basedp_roubo_carga r ON b.cod_ocorrencia = r.cod_ocorrencia '
        'WHERE b.ano IN (2022, 2023)'
    )

    df_roubo = pd.read_sql(query, engine)

    df_ocorrencia_roubo = (
        df_roubo.groupby('munic')['roubo_carga']
        .sum()
        .reset_index()
    )

except Exception as e:
    print(f'Erro: {e}')

try:
    print("\nObtendo informações do roubo de carga.")
    print(30 * '-')
    array_roubo_carga = np.array(df_ocorrencia_roubo['roubo_carga'])
    media_roubo_carga = np.mean(array_roubo_carga)
    mediana_roubo_carga = np.median(array_roubo_carga)
    distancia = abs((media_roubo_carga) / mediana_roubo_carga)
    #   QUARTIS
    q1 = np.quantile(array_roubo_carga, 0.25, method='weibull')
    q2 = np.quantile(array_roubo_carga, 0.50, method='weibull')
    q3 = np.quantile(array_roubo_carga, 0.75, method='weibull')
    #   AMPLITUDE
    maximo = np.max(array_roubo_carga)
    minimo = np.min(array_roubo_carga)
    amplitude = maximo - minimo
    # MUNICÍPIOS COM MAIOR E MENOR NÚMERO DE ROUBOS DE CARGA
    df_ocorrencia_roubo_menores = df_ocorrencia_roubo[
        df_ocorrencia_roubo['roubo_carga'] < q2]
    df_ocorrencia_roubo_maiores = df_ocorrencia_roubo[
        df_ocorrencia_roubo['roubo_carga'] > q3]
    #   IQR
    iqr = q3 - q1
    #   limite superior e inferior
    limite_superior = q3 + 1.5 * iqr
    limite_inferior = q1 - 1.5 * iqr
    #   MEDIDAS DE DISPERSÃO
    variancia = np.var(array_roubo_carga)
    desvio_padrao = np.std(array_roubo_carga)
    distancia_var_media = (variancia / media_roubo_carga ** 2)
    coef_variacao = (desvio_padrao / media_roubo_carga) * 100
    #   ASSIMETRIA
    assimetria = df_ocorrencia_roubo['roubo_carga'].skew()
    #   CURTOSE
    curtose = df_ocorrencia_roubo['roubo_carga'].kurtosis()

    print("\nMenores registros de Roubos de Carga: ")
    print(30 * '-')
    print(df_ocorrencia_roubo_menores.sort_values(by='roubo_carga', ascending=True))
    print("\nMaiores registros de Roubos de Carga: ")
    print(30 * '-')
    print(df_ocorrencia_roubo_maiores.sort_values(by='roubo_carga', ascending=False))
    #   printando as medidas
    print(f'\nMEDIDAS')
    print(30 * '-')
    print(f'limite inferior: {limite_inferior}')
    print(f'Minimo: {minimo}')
    print(f'Quartil 1: {q1}')
    print(f'Quartil 2: {q2}')
    print(f'Quartil 3: {q3}')
    print(f'IQR: {iqr}')
    print(f'Máximo: {maximo}')
    print(f'limite superior: {limite_superior}')
    print(f'Amplitude: {amplitude}')
    print(f'Média: {media_roubo_carga:.2f}')
    print(f'Mediana: {mediana_roubo_carga}')
    print(f'Distância entre Média e Mediana: {distancia:.2f}')
    print(f'Variância: {variancia:.2f}')
    print(f'Desvio Padrão: {desvio_padrao:.2f}')
    print(f'Distância Variância/Média: {distancia_var_media:.2f}')
    print(f'Coeficiente de Variação: {coef_variacao:.2f}%')
    print(f'Assimetria: {assimetria:.2f}')
    print(f'Curtose: {curtose:.2f}')
    #   OUTLIERS
    df_roubo_carga_outliers_inferiores = df_ocorrencia_roubo[
        df_ocorrencia_roubo['roubo_carga'] < limite_inferior]
    df_roubo_carga_outliers_superiores = df_ocorrencia_roubo[
        df_ocorrencia_roubo['roubo_carga'] < limite_superior]

    print('\nMunicípios com outliers inferiores: ')
    print(45*'-')
    if len(df_roubo_carga_outliers_inferiores) == 0:
        print('Não existem outliers inferiores!')
    else:
        print(df_roubo_carga_outliers_inferiores.sort_values(by='roubo_carga', ascending=True))

    print('\nMunicípios com outliers superiores: ')
    print(45*'-')
    if len(df_roubo_carga_outliers_superiores) == 0:
        print('Não existe outliers superiores!')
    else:
        print(df_roubo_carga_outliers_superiores.sort_values(by='roubo_carga', ascending=False))

except Exception as e:
    print(f'Erro ao obter informações do roubo de carga: {e}')
    exit()
# Plotando os dados
try:
    # import matplotlib.pyplot as plt
    plt.subplots(2, 2, figsize=(18, 12))
    plt.suptitle('Análise de roubo de cargas RJ')

    # POSIÇÃO 01
    # BOXPLOT
    plt.subplot(2, 2, 1)  
    plt.boxplot(array_roubo_carga, vert=False, showmeans=True)
    plt.title("Boxplot dos Dados")

    # POSIÇÃO 02
    # MEDIDAS
    plt.subplot(2, 2, 2)
    plt.title('Medidas Estatísticas')
    plt.text(0.1, 0.9, f'Limite inferior: {limite_inferior}', fontsize=10)
    plt.text(0.1, 0.8, f'Menor valor: {minimo}', fontsize=10)
    plt.text(0.1, 0.7, f'Q1: {q1}', fontsize=10)
    plt.text(0.1, 0.6, f'Mediana: {mediana_roubo_carga}', fontsize=10)
    plt.text(0.1, 0.5, f'Q3: {q3}', fontsize=10)
    plt.text(0.1, 0.4, f'Média: {media_roubo_carga:.3f}', fontsize=10)
    plt.text(0.1, 0.3, f'Maior valor: {maximo}', fontsize=10)
    plt.text(0.1, 0.2, f'Limite superior: {limite_superior}', fontsize=10)

    plt.text(0.5, 0.9, f'Distância Média e Mediana: {distancia:.3f}', fontsize=10)
    plt.text(0.5, 0.8, f'IQR: {iqr}', fontsize=10)
    plt.text(0.5, 0.7, f'Amplitude Total: {amplitude}', fontsize=10)
    plt.text(0.5, 0.6, f'Variância: {variancia:.3f}', fontsize=10)
    plt.text(0.5, 0.5, f'Distancia da média e Variância: {distancia_var_media}', fontsize=9)
    plt.text(0.5, 0.4, f'Coeficiente de Variação: {coef_variacao}', fontsize=10)
    plt.text(0.5, 0.3, f'Desvio Padrão: {desvio_padrao:.3f}', fontsize=10)

    plt.xticks([])
    plt.yticks([])

    # POSIÇÃO 03
    # HISTOGRAMA DA DISTRIBUIÇÃO
    plt.subplot(2, 2, 3)
    plt.title('Concentração das Distribuições')
    # Histograma
    plt.hist(array_roubo_carga, bins=77, edgecolor='violet')

    # POSIÇÃO 04
    # OUTLIERS SUPERIORES # diminuindo os valores para evitar que o gráfico fique muito grande

    plt.subplot(2, 2, 4)
    plt.title('Outliers Superiores')
    if not df_roubo_carga_outliers_superiores.empty:
        dados_superiores = df_roubo_carga_outliers_superiores.sort_values(by='roubo_carga', ascending=True)

        # Cria o gráfico e guarda as barras
        barras = plt.barh(dados_superiores['munic'], dados_superiores['roubo_carga'], color='green')
        # Adiciona rótulos nas barras
        plt.bar_label(barras, fmt='%.0f', label_type='edge', fontsize=6, padding=3)

        # Diminui o tamanho da fonte dos eixos
        plt.xticks(fontsize=5)
        plt.yticks(fontsize=5)

        plt.title('Outliers Superiores')
        plt.xlabel('Total Roubos de cargas')    
    else:
        # Se não houver outliers superiores, exibe uma mensagem no lugar.
        plt.text(0.5, 0.5, 'Sem outliers superiores', ha='center', va='center', fontsize=12)
        plt.title('Outliers Superiores')
        plt.xticks([])
        plt.yticks([])

    # Ajusta os espaços do layout para que os gráficos não fiquem espremidos
    plt.tight_layout()
    # Mostra o painel
    plt.show()

except Exception as e:
    print(f'Erro ao plotar {e}')
    exit()
# These are recently edited files. Do not suggest code that has been deleted.