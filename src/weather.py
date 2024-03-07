import datetime, random, time

def weather_trends_generator():
    trend_values = [28, 37, 42, 36, 32, 26, 38, 41, 40, 39, 42, 28, 29, 38, 50, 49, 49, 35, 37, 32, 31, 42, 39, 41, 30, 33, 43, 39, 42, 43, 42, 44, 24, 28, 36, 48, 40, 41, 53, 47, 38, 35, 53, 50, 23, 29, 31, 20, 40, 27, 32, 43, 35, 38, 30, 15, 30, 34, 36, 44, 52, 29, 36, 30, 35, 39, 38, 43, 44, 58, 75, 71, 69, 64, 58, 40, 42, 33, 48, 55, 47, 31, 37, 45, 41, 50, 54, 43, 45, 55, 57, 39, 55, 60, 43, 36, 46, 66, 59, 50, 39, 45, 54, 61, 56, 54, 46, 57, 55, 66, 69, 72, 82, 85, 85, 84, 82, 64, 68, 61, 57, 64, 62, 48, 65, 68, 77, 77, 63, 49, 64, 53, 69, 65, 55, 69, 56, 69, 53, 51, 62, 69, 67, 69, 60, 69, 74, 76, 66, 71, 75, 83, 76, 62, 65, 62, 76, 77, 78, 79, 80, 75, 90, 84, 83, 84, 76, 85, 82, 74, 84, 79, 70, 76, 81, 84, 86, 83, 83, 79, 89, 79, 82, 91, 94, 83, 74, 77, 90, 82, 83, 79, 74, 74, 72, 75, 81, 86, 84, 79, 81, 80, 80, 78, 82, 84, 83, 78, 86, 82, 77, 76, 77, 80, 85, 84, 75, 72, 80, 81, 80, 82, 80, 77, 75, 82, 86, 82, 79, 91, 73, 66, 68, 78, 78, 83, 86, 81, 92, 88, 81, 82, 80, 87, 83, 78, 92, 85, 93, 79, 80, 82, 84, 80, 84, 84, 73, 74, 62, 63, 63, 68, 70, 69, 57, 58, 71, 79, 79, 83, 68, 56, 60, 67, 72, 73, 67, 81, 79, 55, 45, 47, 43, 58, 59, 60, 62, 57, 67, 74, 46, 50, 62, 52, 53, 59, 55, 49, 56, 58, 50, 63, 68, 67, 72, 67, 60, 39, 36, 40, 36, 36, 37, 48, 49, 43, 45, 67, 68, 61, 52, 52, 57, 56, 64, 53, 49, 51, 49, 65, 65, 42, 37, 49, 49, 37, 32, 24, 35, 33, 36, 43, 47, 39, 42, 45, 34, 32, 35, 31, 34, 33, 33, 38, 35, 14, 1, 17, 20, 7, 27, 35, 34, 7, 15]
    start = datetime.date(1990, 1, 1)
    counter = 0

    while True:
        anomaly_chance = random.random()
        if counter > 0 and anomaly_chance < 0.05:
            anomalous_date = start + datetime.timedelta(days=counter-1)
            yield anomalous_date.strftime('%Y-%m-%d'), 1000000
            continue

        current_year = start.year + (counter // 365)
        day_in_year = counter % 365 + 1

        leap_year_adjustment = 0
        if current_year % 4 == 0 and (current_year % 100 != 0 or current_year % 400 == 0):
            leap_year_adjustment = 1 if day_in_year > 60 else 0

        temp_index = day_in_year - 1 + leap_year_adjustment - (1 if leap_year_adjustment else 0)
        daily_temp = trend_values[temp_index]
        
        yearly_increase = (current_year - 1990) * random.uniform(0.02, 0.1)
        daily_variation = random.uniform(-5, 5)
        temperature = round(daily_temp + yearly_increase + daily_variation, 4)

        yield_date = start + datetime.timedelta(days=counter)
        yield yield_date.strftime('%Y-%m-%d'), temperature
        counter += 1

def generate_weather(delay_seconds=1):
    generator = weather_trends_generator()
    while True:
        weather_data = next(generator)
        yield weather_data
        time.sleep(delay_seconds)
