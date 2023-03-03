from datetime import datetime, timedelta
import pytz

# Convert date unaware New York time to UTC
def convert_new_york_datetime_to_utc(ny_datetime):
    return pytz.timezone("America/New_York").localize(ny_datetime).astimezone(pytz.timezone("UTC"))

# Return df of the dates of upcoming Monday-Sunday given a reference date that is the Tuesday-Sunday of the prior week
def upcoming_7_dates_starting_monday(
    date_starting_monday # this is the date of the Monday that we want to start our 7 upcoming dates from
):

    import pandas as pd

    # Confirm reference_date is Monday, otherwise raise exception
    if date_starting_monday.weekday() != 0:
        raise Exception("date_starting_monday needs to be a Monday.")        

    # # Sanity check: date_starting_monday is less than 6 days in the future or past, otherwise raise exception
    # date_difference = date_starting_monday - datetime.today()

    # if abs(date_difference.days) >= 6:
    #     raise Exception("date_starting_monday is more than 6 days in the future or past. Make sure that date_starting_monday is the Monday of the week you want to schedule messages for.")

    # Create list of number of days between Monday and each day of the week
    list_day_deltas = [
        ['Mo',0],
        ['Tu',1],
        ['We',2],
        ['Th',3],
        ['Fr',4],
        ['Sa',5],
        ['Su',6]
    ]

    list_upcoming_dates = []

    # Calculate the date for each upcoming day of the week
    for day in list_day_deltas:

        date_upcoming = date_starting_monday + timedelta(days=day[1])

        list_upcoming_dates.append(
            [day[0], date_upcoming.year, date_upcoming.month, date_upcoming.day] 
        )

    # Create dataframe of upcoming dates
    df_upcoming_dates = pd.DataFrame(
        list_upcoming_dates, 
        columns=['day_of_week', 'year', 'month', 'day'])

    return df_upcoming_dates

# Return df of the dates of upcoming Monday-Sunday given a reference date that is the Tuesday-Sunday of the prior week
def upcoming_7_dates_starting_upcoming_monday(
    reference_date = datetime.now(pytz.timezone("UTC"))
):

    import pandas as pd

    # Confirm reference_date is not Monday, otherwise raise exception because we need Monday to be in the future
    if reference_date.weekday() == 0:
        raise Exception("reference_date is a Monday. We want to return the next Monday, so that doesn't work. If today is Monday and you want to return today as the first date, then you can set reference_date to yesterday.")        

    # Identify the date of the upcoming Monday
    date_upcoming_monday = reference_date + timedelta(days=-reference_date.weekday(), weeks=1) 

    # Create list of number of days between Monday and each day of the week
    list_day_deltas = [
        ['Mo',0],
        ['Tu',1],
        ['We',2],
        ['Th',3],
        ['Fr',4],
        ['Sa',5],
        ['Su',6]
    ]

    list_upcoming_dates = []

    # Calculate the date for each upcoming day of the week
    for day in list_day_deltas:

        date_upcoming = date_upcoming_monday + timedelta(days=day[1])

        list_upcoming_dates.append(
            [day[0], date_upcoming.year, date_upcoming.month, date_upcoming.day] 
        )

    # Create dataframe of upcoming dates
    df_upcoming_dates = pd.DataFrame(
        list_upcoming_dates, 
        columns=['day_of_week', 'year', 'month', 'day'])

    return df_upcoming_dates