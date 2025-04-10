import plotly.express as px
import pycountry
import pycountry_convert as pcc
from fuzzywuzzy import process
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lag, sum as spark_sum, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('End to end processing').getOrCreate()

# Load and clean data
df = spark.read.csv('input/visa_number_in_japan.csv', header=True, inferSchema=True)
new_column_names = [col_name.replace(' ', '_').replace('/', '').replace('.', '').replace(',', '') for col_name in df.columns]
df = df.toDF(*new_column_names).dropna(how='all')
df = df.select('year', 'country', 'number_of_issued_numerical')

# Country name correction
def correct_country_name(name, threshold=85):
    countries = [country.name for country in pycountry.countries]
    corrected_name, score = process.extractOne(name, countries)
    return corrected_name if score >= threshold else name

def get_continent_name(country_name):
    try:
        code = pcc.country_name_to_country_alpha2(country_name, cn_name_format='default')
        cont = pcc.country_alpha2_to_continent_code(code)
        return pcc.convert_continent_code_to_continent_name(cont)
    except:
        return None

correct_country_name_udf = udf(correct_country_name, StringType())
df = df.withColumn('country', correct_country_name_udf(df['country']))

country_corrections = {
    'Andra': 'Russia', 'Antigua Berbuda': 'Antigua and Barbuda', 'Barrane': 'Bahrain',
    'Brush': 'Bhutan', 'Komoro': 'Comoros', 'Benan': 'Benin', 'Kiribass': 'Kiribati',
    'Gaiana': 'Guyana', 'Court Jiboire': "Côte d'Ivoire", 'Lesot': 'Lesotho',
    'Macau travel certificate': 'Macao', 'Moldoba': 'Moldova', 'Naure': 'Nauru',
    'Nigail': 'Niger', 'Palao': 'Palau', 'St. Christopher Navis': 'Saint Kitts and Nevis',
    'Santa Principa': 'Sao Tome and Principe', 'Saechel': 'Seychelles', 'Slinum': 'Saint Helena',
    'Swaji Land': 'Eswatini', 'Torque menistan': 'Turkmenistan', 'Tsubaru': 'Zimbabwe',
    'Kosovo': 'Kosovo'
}

continent_udf = udf(get_continent_name, StringType())
df = df.replace(country_corrections, subset='country')
df = df.withColumn('continent', continent_udf(df['country']))
df.createGlobalTempView('japan_visa')

# Line chart: Yearly visa issuance by continent
df_cont = spark.sql("""
    SELECT year, continent, sum(number_of_issued_numerical) AS visa_issued
    FROM global_temp.japan_visa
    WHERE continent IS NOT NULL
    GROUP BY year, continent
    ORDER BY year, continent
""").toPandas()

fig = px.line(df_cont, x='year', y='visa_issued', color='continent')
fig.update_layout(title='Visa Issuance Trends in Japan (2006–2017) by Continent',
                  xaxis_title='Year', yaxis_title='Number of Visas Issued')
fig.write_html('output/visa_number_in_japan_continent_2006_2017.html')

# Bar chart: Top 10 countries in 2017
df_country = spark.sql("""
    SELECT country, sum(number_of_issued_numerical) AS visa_issued
    FROM global_temp.japan_visa
    WHERE country NOT IN ('total', 'others') AND country IS NOT NULL AND year = 2017
    GROUP BY country
    ORDER BY visa_issued DESC
    LIMIT 10
""").toPandas()

fig = px.bar(df_country, x='country', y='visa_issued', color='country')
fig.update_layout(title='Top 10 countries with most issued visa in 2017',
                  xaxis_title='Country', yaxis_title='Number of visa issued')
fig.write_html('output/visa_number_in_japan_by_country_2017.html')

# Choropleth map
df_map = spark.sql("""
    SELECT year, country, sum(number_of_issued_numerical) AS visa_issued
    FROM global_temp.japan_visa
    WHERE country NOT IN ('total', 'others') AND country IS NOT NULL
    GROUP BY year, country
    ORDER BY year ASC
""").toPandas()

fig = px.choropleth(df_map, locations='country', color='visa_issued',
                    hover_name='country', animation_frame='year',
                    range_color=[100000, 100000],
                    color_continuous_scale=px.colors.sequential.Plasma,
                    locationmode='country names',
                    title='Yearly visa issued by countries')
fig.write_html('output/visa_number_in_japan_year_map.html')

# Line chart: Growth rate by continent
df_growth = spark.sql("""
    SELECT year, continent, sum(number_of_issued_numerical) AS visa_issued
    FROM global_temp.japan_visa
    WHERE continent IS NOT NULL
    GROUP BY year, continent
""")

window_spec = Window.partitionBy('continent').orderBy('year')
df_growth = df_growth.withColumn('prev_year', lag('visa_issued').over(window_spec))
df_growth = df_growth.withColumn('growth_rate', ((col('visa_issued') - col('prev_year')) / col('prev_year')) * 100)
df_growth = df_growth.dropna()
fig = px.line(df_growth.toPandas(), x='year', y='growth_rate', color='continent')
fig.update_layout(title='Year-over-Year Visa Growth Rate by Continent', xaxis_title='Year', yaxis_title='Growth Rate (%)')
fig.write_html('output/visa_growth_rate_by_continent.html')

# Heatmap: Visa issuance by year and continent
df_heatmap = df_cont.pivot(index='continent', columns='year', values='visa_issued')
fig = px.imshow(df_heatmap,
                labels=dict(x='Year', y='Continent', color='Visa Issued'),
                title='Visa Issuance Heatmap by Continent and Year')
fig.write_html('output/visa_heatmap_by_continent_year.html')

# Peak year for each continent
df_peak = spark.sql("""
    SELECT year, continent, sum(number_of_issued_numerical) AS visa_issued
    FROM global_temp.japan_visa
    WHERE continent IS NOT NULL
    GROUP BY year, continent
""")

ranked = df_peak.withColumn('rank', row_number().over(Window.partitionBy('continent').orderBy(col('visa_issued').desc())))
df_peak_year = ranked.filter(col('rank') == 1).select('continent', 'year', 'visa_issued').toPandas()
fig = px.funnel(df_peak_year, x='visa_issued', y='continent', color='continent', text='year')
fig.update_layout(title='Peak Year of Visa Issuance by Continent (Funnel Chart)', xaxis_title='Number of Visas Issued', yaxis_title='Continent')
fig.write_html('output/visa_peak_year_by_continent.html')

# Save cleaned dataset
df.write.csv("output/visa_number_in_japan_cleaned.csv", header=True, mode='overwrite')
spark.stop()

