import pandas as pd
import streamlit as st
import plotly.graph_objects as go

st.set_page_config(layout="wide")
# All necessary URLs
jar_url = 'https://www.registrucentras.lt/aduomenys/?byla=JAR_IREGISTRUOTI.csv'
jar_url_xlsx = 'https://www.registrucentras.lt/aduomenys/?byla=JAR_IREGISTRUOTI.xlsx'
apskritys_url = 'https://www.registrucentras.lt/aduomenys/?byla=adr_apskritys.csv'
savivaldybes_url = 'https://www.registrucentras.lt/aduomenys/?byla=adr_savivaldybes.csv'
seniunijos_url = 'https://www.registrucentras.lt/aduomenys/?byla=adr_seniunijos.csv'
vietoves_url = 'https://www.registrucentras.lt/aduomenys/?byla=adr_gyvenamosios_vietoves.csv'
gatves_url = 'https://www.registrucentras.lt/aduomenys/?byla=adr_gatves.csv'

# Abbreviations used for parsing later on
abbreviations = [
    'a.', 'aklg.', 'al.', 'aplink.', 'g.', 'kel.', 'krant.', 'pl.', 'pr.', 'skg.', 'skv.', 'tak.'
]


# # Function to remove zipcode from the address. This makes parsing the address less complex
def remove_zipcode(text):
    if pd.isna(text):
        return text
    elif "LT-" in str(text):
        return text[:text.rfind(",")] if ',' in str(text) else text
    else:
        return text


# Function to parse actual values of Vietove and Gatve
def get_real_values(row):
    vietove = row['Vietove']
    gatve = row['Gatve']

    if pd.isna(gatve):
        if pd.isna(vietove):
            return None, None
        for abbr in abbreviations:
            if abbr in vietove:
                return None, vietove.split(abbr)[0].strip()
        return vietove, gatve
    else:
        # Handle cases where seniunija is specified
        if 'sen.' in vietove:
            temp_gatve = row['adresas'].split(',')[-1].strip()
            for abbr in abbreviations:
                if abbr in temp_gatve:
                    gatve = temp_gatve.split(abbr)[0].strip()
                else:
                    gatve = temp_gatve.split(' ')[0].strip()
            vietove = row['adresas'].split(',')[-2].strip()
        else:
            gatve = gatve.split(' ')[0].strip()
        return vietove, gatve


@st.cache_data
# Function for import and combining data
def import_and_combine_data():

    # Import jar data
    try:
        jar_df = pd.read_csv(jar_url)
    # Handle inconsistent data
    except pd.errors.ParserError as e:
        print("Parser error:", e)
        jar_df = pd.read_excel(jar_url_xlsx)

    # Drop rows, which do not contain an address (found one of those)
    jar_df.dropna(subset=['adresas'], inplace=True)

    # Import address data
    apskritys_df = pd.read_csv(apskritys_url, delimiter='|')
    savivaldybes_df = pd.read_csv(savivaldybes_url, delimiter='|')
    seniunijos_df = pd.read_csv(seniunijos_url, delimiter='|')
    vietoves_df = pd.read_csv(vietoves_url, delimiter='|')
    gatves_df = pd.read_csv(gatves_url, delimiter='|')

    savivaldybes_unique = savivaldybes_df['VARDAS_K'].str.split(expand=True)[0].unique()
    vietoves_temp = vietoves_df[vietoves_df['VARDAS_K'].isin(savivaldybes_unique)]
    vietoves_unique = vietoves_temp.drop_duplicates(['VARDAS'])

    # Combine address data
    merged_df = apskritys_df.merge(savivaldybes_df, how='left', on='ADM_KODAS', suffixes=['_APSKR', '_SAV'])
    merged_df2 = merged_df.merge(seniunijos_df, how='left', on='SAV_KODAS')
    merged_df3 = merged_df2.merge(vietoves_df, how='left', on='SEN_KODAS', suffixes=['_SEN', '_VIET'])
    merged_df4 = merged_df3.merge(gatves_df, how='left', on='GYV_KODAS')

    # Remove zipcode info from adresas column
    jar_df['adresas'] = jar_df['adresas'].apply(remove_zipcode)

    # Split the address into multiple columns
    new_columns = jar_df['adresas'].str.split(', ', expand=True)

    # Assign the new columns to the DataFrame
    jar_df['Savivaldybe'] = new_columns[0]
    jar_df['Vietove'] = new_columns[1]
    jar_df['Gatve'] = new_columns[2]

    # Apply get_real_values function row-wise to create new columns 'Vietove_real' and 'Gatve_real'
    jar_df[['Vietove_real', 'Gatve_real']] = jar_df.apply(get_real_values, axis=1, result_type='expand')

    # Remove old Vietove and Gatve columns
    jar_df.drop(columns=['Vietove', 'Gatve'], inplace=True)

    # Update Savivaldybe names
    jar_df['Savivaldybe'] = jar_df['Savivaldybe'].replace(
        dict(zip(
            vietoves_unique['VARDAS'].to_list(),
            vietoves_unique['VARDAS_K'].to_list()
        ))
    )

    # Map the additional_info to the corresponding city name suffixes
    additional_info_mapping = {
        'miesto savivaldybės teritorija': ' miesto',
        'm. sav.': ' miesto',
        'rajono savivaldybės teritorija': ' rajono',
        'r. sav.': ' rajono'
    }

    # Split the Savivaldybe column into two separate columns: Savivaldybe and additional info
    jar_df[['Savivaldybe', 'additional_info']] = jar_df['Savivaldybe'].str.split(n=1, expand=True)

    # Specify Vietove whenever one isn't found - removes a lot of incorrect duplicates when merging later on
    jar_df['Vietove_real'] = jar_df['Vietove_real'].fillna(jar_df['Savivaldybe'])

    # Modify the 'city' column based on the 'additional_info'
    jar_df['Savivaldybe'] = jar_df['Savivaldybe'] + jar_df['additional_info'].map(additional_info_mapping).fillna('')

    # Drop unnecessary column and some rows, containing None values
    jar_df.drop(columns=['additional_info'], inplace=True)
    jar_df.dropna(subset=['Gatve_real'], inplace=True)

    # Merge JAR and AR data on Savivaldybe and Gatve columns
    df = jar_df.merge(
        merged_df4[['VARDAS_K_APSKR', 'VARDAS_K_SAV', 'VARDAS_K_VIET', 'VARDAS_K']], how='left',
        left_on=['Savivaldybe', 'Gatve_real'], right_on=['VARDAS_K_SAV', 'VARDAS_K']
    )

    return df.copy()


# Import and combine data
final_df = import_and_combine_data()


# Steamlit app logic
def main():
    # Create columns, so that selectboxes are more compact
    col1, col2 = st.columns(2)

    with col1:
        # Selectbox for apskritis
        apskritis_selected = st.selectbox(
            'Pasirinkite Apskritį', final_df['VARDAS_K_APSKR'].dropna().sort_values(ascending=True).unique()
        )
        df_apskritis = final_df[final_df['VARDAS_K_APSKR'] == apskritis_selected]
        apskritis_count = len(df_apskritis)

        # Selectbox for savivaldybe
        savivaldybe_selected = st.selectbox('Pasirinkite Savivaldybę', df_apskritis['Savivaldybe'].sort_values(ascending=True).unique())
        df_savivaldybe = df_apskritis[df_apskritis['Savivaldybe'] == savivaldybe_selected]
        savivaldybe_count = len(df_savivaldybe)

    with col2:
        # Selectbox for pavadinimas
        pavadinimai = df_savivaldybe['form_pavadinimas'].sort_values(ascending=True).unique().tolist()
        # If possible, make Uždaroji akcinė bendrovė the default selection for pavadinimas, otherwise - take the first one
        default_index_uab = pavadinimai.index('Uždaroji akcinė bendrovė') if 'Uždaroji akcinė bendrovė' in pavadinimai else 0
        pavadinimas_selected = st.selectbox('Pasirinkite Teisinės Formos Pavadinimą', pavadinimai , index=default_index_uab)
        df_pavadinimas = df_savivaldybe[df_savivaldybe['form_pavadinimas'] == pavadinimas_selected]
        pavadinimas_count = len(df_pavadinimas)

        # Selectbox for metai
        metai_selected = st.selectbox('Pasirinkite Įregistravimo metus(Filtruojama nuo šios datos)', df_pavadinimas['stat_data_nuo'].sort_values(ascending=True).unique())
        df_metai = df_pavadinimas[df_pavadinimas['stat_data_nuo'] >= metai_selected]
        metai_count = len(df_metai)

    # Create figure and add traces
    fig = go.Figure(layout=dict(height=650))
    fig.add_trace(go.Bar(
        x=['Apskritis', 'Savivaldybe', 'Pavadinimas', 'Metai'],
        y=[apskritis_count, savivaldybe_count, pavadinimas_count, metai_count],
        name='Juridinių asmenų skaičius'
    ))

    # Update figure layout
    fig.update_layout(
        title='Juridinių asmenų skaičiaus dinamika pagal pasirinktus parametrus',
        xaxis_title='Paskutinis pasirinktas parametras',
        yaxis_title='Juridinių asmenų skaičius',
        xaxis_title_font=dict(size=25),
        yaxis_title_font=dict(size=25),
        title_font=dict(size=30)
    )
    # Plot figure
    st.plotly_chart(fig, use_container_width=True, height=2500)


if __name__ == '__main__':
    main()
