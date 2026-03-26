import sys
import os
from pathlib import Path

# Adicionar path do diretório OM ao sys.path para imports
project_root = Path(__file__).parent.parent.parent
om_path = project_root / "OM"
if str(om_path) not in sys.path:
    sys.path.insert(0, str(om_path))

from OM.costdrivers_comex_OM import *

'''
mexico 

volumes = 'https://www.banxico.org.mx/CuboComercioExterior/Volumen/seriesproducto'
valores = "https://www.banxico.org.mx/CuboComercioExterior/ValorDolares/seriesproducto"

clicar direito em 'Todos los productos'
explorar hasta nivel
Fraccion

clicar fora com direito
copiar contenido
copiar datos sin processar

'''
# import re
# from playwright.sync_api import Playwright, sync_playwright, expect


# def run(playwright: Playwright) -> None:
#     browser = playwright.chromium.launch(headless=False)
#     context = browser.new_context()
#     page = context.new_page()
#     page.goto("https://www.banxico.org.mx/CuboComercioExterior/Volumen/seriesproducto")
#     page.get_by_role("navigation").filter(has_text="Cubo de Información de").click()
#     page.locator("a").filter(has_text="Cubo de Información de").click()
#     page.locator("[data-test-id=\"visual_1\"]").get_by_text("Todos los productos").click(button="right")
#     page.get_by_text("Fracción", exact=True).click()
#     page.get_by_text("Seleccionada").click()
#     page.get_by_role("checkbox", name="Julio 2025").check()
#     page.get_by_role("checkbox", name="Junio 2025").check()
#     page.get_by_role("checkbox", name="Mayo 2025").check()
#     page.get_by_role("checkbox", name="Abril 2025").check()
#     page.get_by_role("checkbox", name="Marzo 2025").check()
#     page.get_by_role("checkbox", name="Febrero 2025").check()
#     page.get_by_role("checkbox", name="Enero 2025").check()
#     page.locator(".free-position-layout").click()
#     page.locator(".free-position-layout").click(button="right")
#     page.locator("div:nth-child(29)").first.click(button="right")
#     page.get_by_text("Copiar datos sin procesar", exact=True).click()
#     page.locator("div").filter(has_text=re.compile(r"^Exportación$")).nth(1).click()
#     page.get_by_text("Importación").click()
#     page.locator("div:nth-child(22)").first.click(button="right")
#     page.get_by_role("row", name="946.000").click(button="right")
#     page.get_by_role("gridcell", name="Mayo 2025").click(button="right")
#     page.get_by_text("--- Asnos.").click(button="right")
#     page.get_by_role("row", name="1,029.000").click(button="right")
#     page.locator(".free-position-layout").click(button="right")
#     page.get_by_role("row", name="39.000", exact=True).click(button="right")
#     page.get_by_text("{Agosto 2025, Julio 2025,").click(button="right")
#     page.get_by_text("Tabla de Información por").click(button="right")
#     page.locator("[data-test-id=\"visual_1\"] div").filter(has_text=".matrix-grid .left.bw-1b-").nth(3).click(button="right")
#     page.locator("[data-test-id=\"visual_1\"] div").filter(has_text=".matrix-grid .left.bw-1b-").nth(3).click(button="right")
#     page.locator("div:nth-child(58)").click(button="right")
#     page.locator(".matrix-grid > div:nth-child(5)").click(button="right")
#     page.get_by_text("Copiar datos sin procesar", exact=True).click()
    
#     page.goto("https://www.banxico.org.mx/CuboComercioExterior/ValorDolares/seriesproducto")
#     page.get_by_text("Seleccionada").click()
#     page.get_by_role("checkbox", name="Julio 2025").check()
#     page.get_by_role("checkbox", name="Junio 2025").check()
#     page.get_by_role("checkbox", name="Mayo 2025").check()
#     page.get_by_role("checkbox", name="Abril 2025").check()
#     page.get_by_role("checkbox", name="Marzo 2025").check()
#     page.get_by_role("checkbox", name="Febrero 2025").check()
#     page.get_by_role("checkbox", name="Enero 2025").check()
#     page.locator("[data-test-id=\"visual_1\"] div").filter(has_text=".matrix-grid .left.bw-1b-").nth(2).click()
#     page.locator("[data-test-id=\"tpanel_acc334d8-8761-4a00-9caf-d3910db94dcc\"]").get_by_role("img").click()
#     page.locator("[data-test-id=\"visual_1\"]").get_by_text("Todos los productos").click(button="right")
#     page.locator("[data-test-id=\"visual_1\"]").get_by_text("Todos los productos").click(button="right")
#     page.get_by_text("Fracción", exact=True).click()
#     page.locator(".matrix-grid > div:nth-child(5)").click(button="right")
#     page.get_by_text("Copiar datos sin procesar", exact=True).click()
#     page.locator("div").filter(has_text=re.compile(r"^Exportación$")).nth(1).click()
#     page.get_by_text("Importación").click()
#     page.locator(".matrix-grid > div:nth-child(5)").click(button="right")
#     page.get_by_text("Copiar datos sin procesar", exact=True).click()

#     # ---------------------
#     context.close()
#     browser.close()


# with sync_playwright() as playwright:
#     run(playwright)

files = ['valores_export.html', 'volumes_export.html', 'valores_import.html', 'volumes_import.html']

dfs_val = []
dfs_pes = []
for file in files:
    df = pd.read_html(file, header=0, decimal='.', thousands=',')[0]
    df['ImportExport'] = 'export' if 'export' in file else 'import'
    #
    if 'valores' in file:
        df.columns = ['Data', 'NCM', 'Valor', 'ImportExport']
        dfs_val.append(df)
    elif 'volumes' in file:
        df.columns = ['Data', 'NCM', 'Peso', 'ImportExport']
        dfs_pes.append(df)

df_valores = pd.concat(dfs_val, ignore_index=True)
df_valores.dropna(subset=['Valor'], inplace=True)

df_volumes = pd.concat(dfs_pes, ignore_index=True)
df_volumes.dropna(subset=['Peso'], inplace=True)

df = df_valores.merge(df_volumes, on=['Data', 'NCM', 'ImportExport'])
df[['mes', 'ano']] = df['Data'].str.split(' ', n=1, expand=True)
df['mes'] = df['mes'].map({'Enero': '01', 'Febrero': '02', 'Marzo': '03', 'Abril': '04', 'Mayo': '05', 'Junio': '06', 'Julio': '07', 'Agosto': '08', 'Septiembre': '09', 'Octubre': '10', 'Noviembre': '11', 'Diciembre': '12'})

df['Data'] = pd.to_datetime(df['ano'] + '-' + df['mes'], format='%Y-%m')
df.drop(columns=['ano', 'mes'], inplace=True)

df['FOB'] = df['Valor'] / df['Peso']

df['ImportExport'] = df['ImportExport'].map({'import': 1, 'export': 0})
df['NCM'] = df['NCM'].str.split(' ', n=1).str[0]
df = df[['NCM', 'ImportExport', 'Data', 'FOB']]

df_ids = pd.read_excel('IDS_comex.xlsx')
ids =  df_ids[(df_ids['pais_1'] == 'México')].copy()

df['NCM'] = df['NCM'].astype(dtype=str)
df['NCM'] = df['NCM'].astype(dtype='int64')
df['ImportExport'] = df['ImportExport'].astype(dtype=int)

ids['NCM'] = ids['NCM'].astype(dtype=str)
ids['NCM'] = ids['NCM'].astype(dtype='int64')
ids['ImportExport'] = ids['ImportExport'].astype(dtype=int)

df_mg = df.merge(ids[['NCM', 'ImportExport', 'IndicePrincipalID']], on=['NCM', 'ImportExport'])
df_mg.rename(
    columns={
        'IndicePrincipalID': 'ID',
        'FOB': 'Valor'
    },
    inplace=True
)

df_mg = df_mg[['ID', 'Data', 'Valor']]
df_mg.drop_duplicates(subset=['ID', 'Data'], inplace=True)

df_resultado = calculate_variation(df_mg)
dataframe_costdriver = get_costdrivers_data(df_mg)

df = calculate_new_values(df_resultado, dataframe_costdriver)
df_preenchido = preenche_lacuna(df)

df_preenchido = df_preenchido[df_preenchido.groupby('ID')['Data'].transform('count') > 1]

# date = datetime.now().strftime('%Y%m%dT%H%M')
# df_preenchido.to_excel(f'subir_COMEX_MEX{date}.xlsx', index=False)
pass