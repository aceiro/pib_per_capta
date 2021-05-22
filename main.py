import pandas as pd
#####################################
#           constans
#####################################
# Links
# https://realpython.com/iterate-through-dictionary-python/
# https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#excelfile-class
# https://www.datacamp.com/community/tutorials/python-string-format?utm_source=adwords_ppc&utm_campaignid=1455363063&utm_adgroupid=65083631748&utm_device=c&utm_keyword=&utm_matchtype=b&utm_network=g&utm_adpostion=&utm_creative=332602034361&utm_targetid=dsa-429603003980&utm_loc_interest_ms=&utm_loc_physical_ms=1001764&gclid=Cj0KCQjw16KFBhCgARIsALB0g8KRXVGBLiWz23NYxn--Ac0oJyACg3b08_U9i_YColBzvEU_bdJezMoaAp05EALw_wcB
# https://stackoverflow.com/questions/27975069/how-to-filter-rows-containing-a-string-pattern-from-a-pandas-dataframe/27975230#27975230
# https://pandas.pydata.org/docs/reference/api/pandas.Series.str.contains.html?highlight=contains#pandas.Series.str.contains
# https://pandas.pydata.org/docs/getting_started/intro_tutorials/03_subset_data.html
# https://pandas.pydata.org/docs/reference/api/pandas.Series.sum.html


_file = '/home/erik/Workspace/aula-eduarda/base.xlsx'
_excel_file = ''
_data = {}
_data_region_name = {
    'N':'Norte',
    'NE':'Nordeste',
    'S': 'Sul',
    'SE': 'Sudeste',
    'CO': 'Centro-Oeste'
}
_debug = False

#####################################
#           methods
#####################################
def debug(m):
    if(_debug):
        print(m)

def load_base_excel():
    debug('Loading data from XLSX')
    with pd.ExcelFile(_file) as xls:
        _data['Populacao_Estado'] = pd.read_excel(xls, "Populacao_Estado", index_col=None, na_values=["NA"])
        _data['De_para_UF'] = pd.read_excel(xls, "De_para_UF", index_col=None, na_values=["NA"])
        _data['PIB_municipio'] = pd.read_excel(xls, "PIB_municipio", index_col=None, na_values=["NA"])
        _data['UF_Regiao'] = pd.read_excel(xls, "UF_Regiao", index_col=None, na_values=["NA"])
    
    debug(_data)

def print_summary():
    # TODO: 1. Adicionar formatação do header de saída. Tab = 4 espaços ... 2x4=8
    #       2. Adicionar formatação para a tabela em geral
    #       3. Adicionar formatação para os valores em ponto-fluante

    debug('Método - Imprime resultado da tabela esperada')
    between_width=' '*8
    print("Regiao{between}PIB_per_capita".format(between=between_width))
    total = 0
    for region_key in _data_region_name:
        sub_total = 0
        region = _data_region_name[region_key]
        pib_per_region = summarize_pib_by_region(region_key)
        sub_total += pib_per_region['pib_per_capta']
        total += sub_total
        print("{region:<12}{between}{pib}".format(region=region, pib=sub_total, between=between_width))

    print("Total{between:<15}{total}".format(between=between_width, total=total))

def summarize_pib_by_region(region=''):
    states = states_by_region(region)
    sub_total_pib_by_region = 0
    sub_total_population = 0
    pib_per_capta = 0
    for state in states:
        code  = code_by_state(state)
        pibs  = pibs_by_code(code)
        sub_total_pib_by_region += calculate_total_pib(pibs)
        sub_total_population +=calculate_total_population_by_state(state)
        pib_per_capta = round(sub_total_pib_by_region / sub_total_population,2)
    return {'sub_total_pib_by_region':sub_total_pib_by_region, 'sub_total_population': sub_total_population, 'pib_per_capta':pib_per_capta}


def calculate_total_population_by_state(state=''):
    populations = populations_by_state(state)
    sub_total_population = calculate_total_population(populations)
    return sub_total_population

#  métodos de funil
#  Pipeline de Processamento e Testes
# 
#   I) pre-processamento >> subset de dados >> calculo Total PIB por região >> saída em tela
#  II) pre-processamento >> subset de dados >> calculo Total PIB por região >> calculo do PIB per capta >> saída em tela
#  
# III) pre-processamento >> test de funiel (validação por revisão dos dados saída script X planilha original)

def states_by_region(region):
    df = pd.DataFrame(_data['UF_Regiao'])
    
    if(region=='N'):
        region_n = df[df['Regiao'].str.contains('^N$',regex=True)]
        return region_n['Estado']
        # TODO: 4. Adicionar outras regioes aqui
        #       5. Modifcar a RegEx para cada uma
    return []

def code_by_state(state):
    df = pd.DataFrame(_data['De_para_UF'])
    state_regex = "^{state}$".format(state = state)
    state_code = df[df['Granularidade'].str.contains(state_regex, regex=True)]
    return state_code['Cod_Identificacao'].values[0]

def pibs_by_code(code):
    df = pd.DataFrame(_data['PIB_municipio']).dropna()
    pibs = df[round(df['Cod_Identificacao']/100000).astype(int)==code]
    return pibs['PIB']

def calculate_total_pib(df_series_pibs = {}):
    return df_series_pibs.sum()

def populations_by_state(state):
    df = pd.DataFrame(_data['Populacao_Estado'])
    state_regex = "^{state}$".format(state = state)
    populations = df[df['Granularidade'].str.contains(state_regex, regex=True)]
    return populations['Populacao'][1:]

def calculate_total_population(df_series_states = {}):
    return df_series_states.sum()

def test_funnel(region='N'):
    load_base_excel()
    states = states_by_region(region)
    debug(states)
    for state in states:
        code  = code_by_state(state)
        pibs  = pibs_by_code(code)
        total = calculate_total_pib(pibs)
        populations = populations_by_state(state)
        debug(calculate_total_population(populations))



#####################################
#           main function
#####################################
def main():
    print ('Objetivo: Cálculo do PIB per capital por macrorregião do Brasil (Norte, Nordeste, Sudeste, Sul e Centro-Oeste) e o total do país')
    load_base_excel()
    print_summary()
    
    
main()

# Test
# método test_funnel() usado para testar o comportamento de refinamento dos dados
# test_funnel()
