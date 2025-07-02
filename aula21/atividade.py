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


host = 'localhost'
user = 'root'
password = ''
database = 'bd_roubos_cargas'


try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        
        query = 'SELECT b.ano, b.munic, b.cod_ocorrencia, r.roubo_carga FROM basedp b JOIN basedp_roubo_carga r ON b.cod_ocorrencia = r.cod_ocorrencia WHERE b.ano IN (2022, 2023)'

        df_roubo = pd.read_sql(query, engine)

        df_ocorrencia_roubo = df_roubo.groupby('munic')['roubo_carga'].sum().reset_index()

    

except Exception as e:
        print(f'Erro: {e}')

try:
        print("Obtendo informações do roubo de carga.")

        array_roubo_carga = np.array(df_ocorrencia_roubo['roubo_carga'])


        media_roubo_carga = np.mean(array_roubo_carga)
        mediana_roubo_carga = np.median(array_roubo_carga)
        distancia = abs((media_roubo_carga) / mediana_roubo_carga)
        print(f'distancia: {distancia}')



        q1 = np.quantile(array_roubo_carga, 0.25)
        q2 = np.quantile(array_roubo_carga, 0.50)
        q3 = np.quantile(array_roubo_carga, 0.75)


        iqr = q3 - q1
        limite_inferior = q1 - (iqr * 1.5)
        limite_superior = q3 + (iqr * 1.5)

        print(f'iqr: {iqr}')
        print(f'limite inferior: {limite_inferior}')
        print(f'limite superior: {limite_superior}')

        maximo = np.max(array_roubo_carga)
        minimo = np.min(array_roubo_carga)
        amplitude_total = maximo - minimo

        assimetria = df_ocorrencia_roubo['roubo_carga'].skew()
        curtose = df_ocorrencia_roubo['roubo_carga'].kurtosis()

        df_base_roubo_carga_menores = df_ocorrencia_roubo[df_ocorrencia_roubo['roubo_carga'] < q2]
        df_base_roubo_carga_maiores = df_ocorrencia_roubo[df_ocorrencia_roubo['roubo_carga'] > q3]

        variancia = np.var(array_roubo_carga)
        distancia_variancia_media = variancia / (media_roubo_carga **2)
        desvio_padrao = np.std(array_roubo_carga)
        coeficiente_variacao = desvio_padrao / media_roubo_carga

        df_roubo_carga_outliers_inferiores = df_ocorrencia_roubo[df_ocorrencia_roubo['roubo_carga']<limite_inferior]
        df_roubo_carga_outliers_superiores = df_ocorrencia_roubo[df_ocorrencia_roubo['roubo_carga']<limite_superior]

        # print(f'media {media_roubo_carga:.2f}')
        # print(f'mediana: {mediana_roubo_carga:.2f}')
        # print(f'Distância da media e mediana: {distancia}')

        # print('Q1:', q1)
        # print('Q2:', q2)
        # print('Q3:', q3)

        # print('Maximo:', maximo)
        # print('Minimo:', minimo)
        # print('Amplitude:', amplitude_total)

        print('Maiores:', df_base_roubo_carga_maiores)
        print('Menores:', df_base_roubo_carga_menores)

        print("\nQUARTIS")
        print(f'Q1: {q1}')
        print(f'Q2: {q2}')
        print(f'Q3: {q3}')
        print(f'IQR: {iqr}')
        print(f'Limite inferior: {limite_inferior}')
        print(f'Limite superior: {limite_superior}')

        print(f'Máximo: {maximo}')
        print(f'Mínimo: {minimo}')
        print(f'Amplitude total: {amplitude_total}')
        print(f'Variancia: {variancia}')
        print(f'Distância entre variância e média: {distancia_variancia_media}')
        print(f'Desvio padrão: {desvio_padrao}')
        print(f'Coeficiente de variação: {coeficiente_variacao}')
        print(f'Assimetria : {assimetria}')
        print(f'Curtose: {curtose}')

        print("\nMENORES ROUBOS")
        print(df_base_roubo_carga_menores.sort_values(by='roubo_carga', ascending=True))
        print("\nMAIORES ROUBOS")
        print(df_base_roubo_carga_maiores.sort_values(by='roubo_carga', ascending=False))

        print("\nOUTLIERS INFERIORES")
        if len(df_roubo_carga_outliers_inferiores) > 0:
            print(df_roubo_carga_outliers_inferiores.sort_values(by='roubo_carga', ascending=False))
        else:
            print("Não há outliers inferiores")

        # OUTLIERS SUPERIORES
        print("\nOUTLIERS SUPERIORES")
        if len(df_roubo_carga_outliers_superiores) > 0:
            print(df_roubo_carga_outliers_superiores.sort_values(by='roubo_carga', ascending=False))
        else:
            print("Não há outliers superiores")
except Exception as e:
        print(f'Erro ao obter informações sobre padrão de roubo de cargas: {e}')
        exit()