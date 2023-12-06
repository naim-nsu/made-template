import pandas as pd

if __name__ == '__main__':
    
    #extract data from the first data source
    for i in range(1,13):
        if(i<10):
            DATA_URL = 'https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_0' + str(i) + '.txt'
        else:    
            DATA_URL = 'https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_' + str(i) + '.txt'
        
        df = pd.read_csv(DATA_URL, sep=";", header=1)
        df = df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df.to_sql("weather", 'sqlite:///data/' + "weather.sqlite", if_exists='append', index=False)
        print("successfully added " + str(i))
        
    
    
    
    
    
    
    