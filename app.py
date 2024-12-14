import streamlit as st
import pandas as pd
from client import get_weather_async
from data_processing import (clean_data, get_stats_by_month, 
                             get_stats_by_season, is_normal_weather)
from datetime import datetime
import asyncio
import matplotlib.pyplot as plt

CITIES = ['New York', 'London', 'Paris', 'Tokyo', 'Moscow', 'Sydney',
          'Berlin', 'Beijing', 'Rio de Janeiro', 'Dubai', 'Los Angeles',
          'Singapore', 'Mumbai', 'Cairo', 'Mexico City']

async def main():
    st.title("Знай погоду по всему миру!")
    st.write("Приложение для получения и анализ данных о погоде.")

    st.header("Шаг 1: Загрузка данных о погоде")

    uploaded_file = st.file_uploader("Выберите CSV-файл", type=["csv"])

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        df = clean_data(df)
        st.write("Файл загружен")
        st.header("Шаг 2: Выбор города")
        selected_city = st.selectbox("Выберите город:", CITIES)
        api_key = st.text_input("Введите ваш API-ключ:", type="password")
        if api_key:
            st.header(f"Шаг 3: Статистика температуры в городе {selected_city}")
            with st.form(key='weather_form'):
                submitted = st.form_submit_button("Получить погоду")

                if submitted:
                    weather_data = await get_weather_async([selected_city], api_key)
                    if isinstance(weather_data, str):
                        st.error(weather_data)
                    else:
                        mean_t_season, std_t_season = get_stats_by_season(df, selected_city)
                        mean_t_month, std_t_month = get_stats_by_month(df, selected_city) 
                        is_normal = is_normal_weather(mean_t_month, std_t_month, weather_data[selected_city])
                        
                        if weather_data:
                            st.write(f"### Текущая погода в **{selected_city}**:")
                            st.write(f"- Температура: **{weather_data[selected_city]:.1f}°C**")

                            st.write("### Средние температуры:")
                            st.write(f"- Средняя температура за месяц: **{mean_t_month:.1f}°C**")
                            st.write(f"- Средняя температура за сезон: **{mean_t_season:.1f}°C**")
                            
                            if is_normal:
                                st.write("### Погода нормальная для данного месяца.")
                            else:
                                st.write("### Погода не является нормальной для данного месяца.")
                            plot_with_outliers(df, selected_city)
                            st.pyplot(plt)
        else:
            st.write("Пожалуйста, введите ваш API-ключ.")

    else:
        st.write("Пожалуйста, загрузите CSV-файл.")

def plot_with_outliers(df, city):
    filtered_df = df.query(f'city == "{city}"')

    plt.figure(figsize=(10, 5))
    plt.plot(filtered_df['timestamp'], filtered_df['temperature'], label='Температура', color='blue')

    outliers = filtered_df[filtered_df['is_outlier_2s']]
    plt.scatter(outliers['timestamp'], outliers['temperature'], color='red', label='Аномалии')

    plt.title(f'Температура в городе {city}')
    plt.xlabel('Дата')
    plt.ylabel('Температура')
    plt.legend()
    plt.grid()

if __name__ == "__main__":
    asyncio.run(main())