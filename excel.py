import pandas
import numpy




#Function definitions
#====================
#Function to tabulate the z-score
def Tabulate_ZScore(df_source_data, row_index, column_index, output_column_index):
    value       = df_source_data.at[row_index, column_index]
    value_mean  = df_source_data[column_index].mean(skipna = True)
    value_std   = df_source_data[column_index].std(skipna = True)
    df_source_data.set_value(row_index, output_column_index, (value - value_mean) / value_std)
#end of function


#Tabulation of percentiles for a given z-score
def Tabulate_ZScore_Percentiles(df_source_data, is_ascending, row_count, column_index, output_column_index):
    df_source_data = df_source_data.sort(columns=column_index, ascending=is_ascending).reset_index(drop=True)
    
    #Count the number of valid rows which are non nan
    count = 0.0
    for row in range(0, row_count):
        if str(df_source_data.at[row, column_index]) != "nan":
            count += 1
        else:
            break
    
    #Assign the percentiles blindly
    for row in range(0, int(count)):
        df_source_data.set_value(row, output_column_index, (count-1-row)/(count-1) * 100)
    
    #Correct percentiles for matching values
    return df_source_data
#end of function


def Tabulate_Indicator(df_source_data, row_count, output_column_index, column_list = []):
    #Append the output column
    df_source[output_column_index] = numpy.nan

    for row in range(0, row_count):
        summation = 0.0
        count = 0.0
        for column_index in column_list:
            if str(df_source_data.at[row, column_index]) != "nan":
                summation += df_source_data.at[row, column_index]
                count += 1        
        if count > 0:
            df_source_data.set_value(row, output_column_index, summation / count)

    return df_source_data
#end of function


def Tabulate_Percentile_Generic(df_source_data, row_count, filter_column, input_column, is_ascending, output_column_index):
    #Sort the data in the data source, based on filter column (country or sector), followed by input column (indicator)
    df_source_data = df_source_data.sort_values([filter_column, input_column], ascending=[True, is_ascending]).reset_index(drop=True)
    #Append the output column
    df_source_data[output_column_index] = numpy.nan

    
    indexer = 0
    temp_indexer = 0
    match_count = 0
    while (indexer < row_count):
        #Search forward to find the number of country/sector matches
        match_count = 1
        for row in range(indexer + 1, row_count):
            #print("comparator, source = " + df_source_data.get_value(df_source_data.index[row], filter_column) + " ," + df_source_data.get_value(df_source_data.index[indexer], filter_column))
            if df_source_data.get_value(df_source_data.index[row], filter_column) == df_source_data.get_value(df_source_data.index[indexer], filter_column):
                match_count += 1
            else:
                break
        #end of for loop
        
        #If this is a solo entry, skip this record and assume 100% percentile
        if match_count == 1:
            df_source_data.set_value(indexer, output_column_index, 1)     #1 = 100%
            indexer += 1
            continue
        
        #Process the tabulations based on the count received.
        # - First, distribute the values evenly across
        for row in range(0, match_count):
            df_source_data.set_value(indexer + row, output_column_index, (float(match_count)-1-row)/(float(match_count)-1))     #Do not multiply: Let excel handle it
        
        #Skip the records that have already been processed by incrementing the loop index
        indexer += match_count
    #end of while loop
    
    #Lastly, keep on upgrading the percentile values while there is a matching country and bucket value.
    #I.e. Values 3, 2, 2, 1, 0 should be percentile 100, 75, 75, 25, 0 respectively
    for row in range(0, row_count - 1):
        if df_source_data.get_value(df_source_data.index[row], filter_column) == df_source_data.get_value(df_source_data.index[row + 1], filter_column) and \
        df_source_data.get_value(df_source_data.index[row], input_column) == df_source_data.get_value(df_source_data.index[row + 1], input_column):
            df_source_data.set_value(row + 1, output_column_index, df_source_data.get_value(df_source_data.index[row], output_column_index))
    return df_source_data
#end of function



#Define Various Constants
#==================================================
# --- Data Dump sheet ---
#General column references for all buckets
DATA_START_ROW = 11
TICKER_COLUMN = "Ticker"
COUNTRY_COLUMN = "Country"
SECTOR_COLUMN = "Sector 1"

#General references
BUCKET_QUALITY_RESULT_COLUMN = 55
BUCKET_VALUATION_RESULT_COLUMN = 66
BUCKET_RISK_RESULT_COLUMN = 77
BUCKET_MOMENTUM_RESULT_COLUMN = 96




#Read in the excel file and its properties
df_source = pandas.read_excel("C:\Users\Zhou En Cheah\Downloads\Data Project New.xlsx", "Data Dump", skiprows = 9, index_col=None, na_values=['N.A.'])
total_rows = len(df_source.index)



#Value Bucket tabulations
#==========================
#Create new columns with empty entries
for new_column in ['zs_PE trail', 'zs_PE fwd', 'zs_PB', 'zs_EV/EBITDA', 'zs_FCF Yield', 'p_PE trail', 'p_PE fwd', 'p_PB', 'p_EV/EBITDA', 'p_FCF Yield']:
    df_source[new_column] = numpy.nan

#Quality Bucket tabulations
#==========================
#Create new columns with empty entries
for new_column in ['zs_ROE', 'zs_NOPAT/Sales', 'zs_FCF/Assets', 'zs_EBIT Margin', 'zs_ROA', 'p_ROE', 'p_NOPAT/Sales', 'p_FCF/Assets', 'p_EBIT Margin', 'p_ROA']:
    df_source[new_column] = numpy.nan

#Risk Bucket tabulations
#==========================
#Create new columns with empty entries
for new_column in ['zs_beta', 'zs_volatility', 'zs_Altman Z', 'zs_Debt/assets', 'zs_Debt/EV', 'p_beta', 'p_volatility', 'p_Altman Z', 'p_Debt/assets', 'p_Debt/EV']:
    df_source[new_column] = numpy.nan

#Momentum Bucket tabulations
#==========================
#Create new columns with empty entries
for new_column in ['zs_1yr change', 'zs_1month change', 'zs_1yr - 1 mo / vol', 'zs_EPS 1w %', 'zs_EPS 4w %', 'zs_EPS 3M %', 'zs_Sales 1w %', 'zs_Sales 4W %', 'zs_Sales 3M %', 'p_1yr change', 'p_1month change', 'p_1yr - 1 mo / vol', 'p_EPS 1w %', 'p_EPS 4w %', 'p_EPS 3M %', 'p_Sales 1w %', 'p_Sales 4W %', 'p_Sales 3M %']:
    df_source[new_column] = numpy.nan

#Tabulate the various z-scores
for row in range(0, total_rows):
    #Value Z-Scores
    Tabulate_ZScore(df_source, row, 'PE trail', 'zs_PE trail')
    Tabulate_ZScore(df_source, row, 'PE fwd', 'zs_PE fwd')
    Tabulate_ZScore(df_source, row, 'PB', 'zs_PB')
    Tabulate_ZScore(df_source, row, 'EV/EBITDA', 'zs_EV/EBITDA')
    Tabulate_ZScore(df_source, row, 'FCF Yield', 'zs_FCF Yield')
    
    #Quality Z-Scores
    Tabulate_ZScore(df_source, row, 'ROE', 'zs_ROE')
    Tabulate_ZScore(df_source, row, 'NOPAT/Sales', 'zs_NOPAT/Sales')
    Tabulate_ZScore(df_source, row, 'FCF/Assets', 'zs_FCF/Assets')
    Tabulate_ZScore(df_source, row, 'EBIT Margin', 'zs_EBIT Margin')
    Tabulate_ZScore(df_source, row, 'ROA', 'zs_ROA')
    
    #Risk Z-Scores
    Tabulate_ZScore(df_source, row, 'beta', 'zs_beta')
    Tabulate_ZScore(df_source, row, 'volatility', 'zs_volatility')
    Tabulate_ZScore(df_source, row, 'Altman Z', 'zs_Altman Z')
    Tabulate_ZScore(df_source, row, 'Debt/assets', 'zs_Debt/assets')
    Tabulate_ZScore(df_source, row, 'Debt/EV', 'zs_Debt/EV')
    
    #Momentum Z-Scores
    Tabulate_ZScore(df_source, row, '1yr change', 'zs_1yr change')
    Tabulate_ZScore(df_source, row, '1month change', 'zs_1month change')
    Tabulate_ZScore(df_source, row, '1yr - 1 mo / vol', 'zs_1yr - 1 mo / vol')
    Tabulate_ZScore(df_source, row, 'EPS 1w %', 'zs_EPS 1w %')
    Tabulate_ZScore(df_source, row, 'EPS 4w % ', 'zs_EPS 4w %')     
    Tabulate_ZScore(df_source, row, 'EPS 3M % ', 'zs_EPS 3M %')    
    Tabulate_ZScore(df_source, row, 'Sales 1w %', 'zs_Sales 1w %')
    Tabulate_ZScore(df_source, row, 'Sales 4W %', 'zs_Sales 4W %')
    Tabulate_ZScore(df_source, row, 'Sales 3M %', 'zs_Sales 3M %')


#Tabulate the various percentiles of z-scores
#Value Z-Scores Percentiles
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_PE trail', 'p_PE trail')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_PE fwd', 'p_PE fwd')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_PB', 'p_PB')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_EV/EBITDA', 'p_EV/EBITDA')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_FCF Yield', 'p_FCF Yield')

#Quality Z-Scores Percentiles
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_ROE', 'p_ROE')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_NOPAT/Sales', 'p_NOPAT/Sales')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_FCF/Assets', 'p_FCF/Assets')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_EBIT Margin', 'p_EBIT Margin')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_ROA', 'p_ROA')

#Risk Z-Scores Percentiles
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_beta', 'p_beta')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_volatility', 'p_volatility')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_Altman Z', 'p_Altman Z')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_Debt/assets', 'p_Debt/assets')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_Debt/EV', 'p_Debt/EV')

#Momentum Z-Scores Percentiles
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_1yr change', 'p_1yr change')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_1month change', 'p_1month change')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_1yr - 1 mo / vol', 'p_1yr - 1 mo / vol')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_EPS 1w %', 'p_EPS 1w %')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_EPS 4w %', 'p_EPS 4w %')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_EPS 3M %', 'p_EPS 3M %')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_Sales 1w %', 'p_Sales 1w %')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_Sales 4W %', 'p_Sales 4W %')
df_source = Tabulate_ZScore_Percentiles(df_source, False, total_rows, 'zs_Sales 3M %', 'p_Sales 3M %')




#Tabulation of Indicators
#====================================
df_source = Tabulate_Indicator(df_source, total_rows, 'in_Valuation', ['p_PE trail', 'p_PE fwd', 'p_PB', 'p_EV/EBITDA', 'p_FCF Yield'])
df_source = Tabulate_Indicator(df_source, total_rows, 'in_Quality', ['p_ROE', 'p_NOPAT/Sales', 'p_FCF/Assets', 'p_EBIT Margin', 'p_ROA'])
df_source = Tabulate_Indicator(df_source, total_rows, 'in_Risk', ['p_beta', 'p_volatility', 'p_Altman Z', 'p_Debt/assets', 'p_Debt/EV'])
df_source = Tabulate_Indicator(df_source, total_rows, 'in_Momentum', ['p_1yr change', 'p_1month change', 'p_1yr - 1 mo / vol', 'p_EPS 1w %', 'p_EPS 4w %', 'p_EPS 3M %', 'p_Sales 1w %', 'p_Sales 4W %', 'p_Sales 3M %'])




#Exporting of data to new sheets
#=========================================
#Create a writer instance to write to excel
writer = pandas.ExcelWriter(r"C:\Users\Zhou En Cheah\Downloads\result.xlsx", engine='xlsxwriter')
# Add some cell formats.
format_percentage = writer.book.add_format({'num_format': '0.0%'})


#Valuation bucket
subset_df_source = df_source[[TICKER_COLUMN, COUNTRY_COLUMN, SECTOR_COLUMN, 'in_Valuation']]
subset_df_source.dropna(inplace=True)                       #strip entries that are nan
subset_df_source = Tabulate_Percentile_Generic(subset_df_source, len(subset_df_source.index), COUNTRY_COLUMN, 'in_Valuation', True, 'Country Percentile')
subset_df_source = Tabulate_Percentile_Generic(subset_df_source, len(subset_df_source.index), SECTOR_COLUMN, 'in_Valuation', True, 'Sector Percentile')
subset_df_source.to_excel(writer, sheet_name='Valuation', index=False)      #Export the data 
writer.sheets['Valuation'].set_column('A:F', 18)
writer.sheets['Valuation'].set_column('E:F', None, format_percentage)
writer.sheets['Valuation'].autofilter("A1:F" + str(len(subset_df_source.index)))

#Quality bucket
subset_df_source = df_source[[TICKER_COLUMN, COUNTRY_COLUMN, SECTOR_COLUMN, 'in_Quality']]
subset_df_source.dropna(inplace=True)                       #strip entries that are nan
subset_df_source = Tabulate_Percentile_Generic(subset_df_source, len(subset_df_source.index), COUNTRY_COLUMN, 'in_Quality', False, 'Country Percentile')
subset_df_source = Tabulate_Percentile_Generic(subset_df_source, len(subset_df_source.index), SECTOR_COLUMN, 'in_Quality', False, 'Sector Percentile')
subset_df_source.to_excel(writer, sheet_name='Quality', index=False)        #Export the data 
writer.sheets['Quality'].set_column('A:F', 18)
writer.sheets['Quality'].set_column('E:F', None, format_percentage)
writer.sheets['Quality'].autofilter("A1:F" + str(len(subset_df_source.index)))

#Risk bucket
subset_df_source = df_source[[TICKER_COLUMN, COUNTRY_COLUMN, SECTOR_COLUMN, 'in_Risk']]
subset_df_source.dropna(inplace=True)                       #strip entries that are nan
subset_df_source = Tabulate_Percentile_Generic(subset_df_source, len(subset_df_source.index), COUNTRY_COLUMN, 'in_Risk', True, 'Country Percentile')
subset_df_source = Tabulate_Percentile_Generic(subset_df_source, len(subset_df_source.index), SECTOR_COLUMN, 'in_Risk', True, 'Sector Percentile')
subset_df_source.to_excel(writer, sheet_name='Risk', index=False)           #Export the data 
writer.sheets['Risk'].set_column('A:F', 18)
writer.sheets['Risk'].set_column('E:F', None, format_percentage)
writer.sheets['Risk'].autofilter("A1:F" + str(len(subset_df_source.index)))

#Momentum bucket
subset_df_source = df_source[[TICKER_COLUMN, COUNTRY_COLUMN, SECTOR_COLUMN, 'in_Momentum']]
subset_df_source.dropna(inplace=True)                       #strip entries that are nan
subset_df_source = Tabulate_Percentile_Generic(subset_df_source, len(subset_df_source.index), COUNTRY_COLUMN, 'in_Momentum', False, 'Country Percentile')
subset_df_source = Tabulate_Percentile_Generic(subset_df_source, len(subset_df_source.index), SECTOR_COLUMN, 'in_Momentum', False, 'Sector Percentile')
subset_df_source.to_excel(writer, sheet_name='Momentum', index=False)       #Export the data 
writer.sheets['Momentum'].set_column('A:F', 18)
writer.sheets['Momentum'].set_column('E:F', None, format_percentage)
writer.sheets['Momentum'].autofilter("A1:F" + str(len(subset_df_source.index)))

#Save the excel file
writer.save()
