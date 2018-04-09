import pandas as pd
import config as cfg
#import s3fs

def process_data():
    f_normed = f'{cfg.DATA_DIR}/bs_normed_full.xls'
    f_normed_agg = f'{cfg.DATA_DIR}/bs_normed_agg.xls'

    basmi_df = pd.read_excel(f'{cfg.DATA_DIR}/clean_basmi.xls', index_col=(0, 1))

    normed_df = normalize_timeline(basmi_df)
    normed_df.to_excel(f_normed)

    # Aggregate the normalized bs data by year and save to disk
    agg_normed_df = (normed_df.groupby('norm_years')
                     .agg({'BS': ['mean','std'], 'norm_years': len})
                     .rename(columns={'norm_years': 'count'}).round(2)
                     )

    agg_normed_df.to_excel(f_normed_agg)


def normalize_timeline(basmi_df, agg_dic={'BS': 'mean'}):
    """
    TODO: Add support for agg dic by including in impute_missing_values function
    
    
    """
    def get_norm_years(df):
        dates = df.index.get_level_values('Date')
        start_date = min(dates)
        norm_years = [int(pd.Timedelta(date - start_date).days / 365) for date in dates]
        return norm_years

    def impute_missing_values(normed_df):
        """
        Impute the missing data for patients who were missing in the study for a year or more
        Assume a linear progression during absence

        :param normed_df:
        :return:
        """
        fixed_dfs = []
        for id, df in normed_df.groupby('patient_id'):

            years = df['norm_years']

            bs_scores = df['BS']

            rate_of_change = (bs_scores.shift(-1) - bs_scores) / (years.shift(-1) - years)

            if df.shape[0] <= 1:
                fixed_df = pd.DataFrame({'BS': bs_scores, 'norm_years': years, 'patient_id': id})
            else:
                bs_scores.index = years
                rate_of_change.index = years

                # Full range of years - the actual years that patient was in the study
                years_range = pd.RangeIndex(0, stop=max(years))

                fixed_data = []
                last_bs_obs = None
                for year in years_range:
                    # If we had data for this year, add set the last observation
                    # and add this entry to fixed data
                    if year in years.values:
                        last_obs = (bs_scores.loc[year], rate_of_change.loc[year])
                        fixed_data.append(last_obs[0])

                    # Else, make a new observation by adding the rate of change to the last BS score we had
                    # and updating the last observation to this new observation keeping the rate of change the same
                    else:
                        new_obs = last_obs[0] + last_obs[1]
                        fixed_data.append(new_obs)
                        last_obs = (new_obs, last_obs[1])

                fixed_df = pd.DataFrame({'BS': fixed_data, 'norm_years': years_range})
                fixed_df['patient_id'] = id

            fixed_dfs.append(fixed_df)

        fixed_bs_df = pd.concat(fixed_dfs)

        fixed_bs_df = fixed_bs_df.set_index('patient_id')

        return fixed_bs_df

    

    # Sub-select BS score
    normed_bs_df = basmi_df.copy()
    # Turn the Drug column into binary
    normed_bs_df['Drug_Indicator'] = normed_bs_df['Drug'].notnull().map({False: 0, True: 1})
    normed_bs_df.drop('Drug', axis=1, inplace=True)
    
    # By subselecting BS here we negate agg_dic
    # TODO - revisit this
    normed_bs_df['norm_years'] = normed_bs_df.groupby(level=0)['BS'].transform(get_norm_years)

    # Bin data per year for each patient
    normed_bs_df = normed_bs_df.groupby(['patient_id', 'norm_years']).agg(agg_dic).reset_index(level=1)
    # Round floats to 2 digits
    normed_bs_df = normed_bs_df.round(2)

    # Fix the missing data
    normed_bs_df = impute_missing_values(normed_bs_df)

    return normed_bs_df



def split_cohorts_by_drugs(basmi_df, demo_df, print_=False, break_at=None):
    """
    Split basmi_df into drugs / no_drugs df
    
    """
    
    if type(basmi_df['Drug']) != bool:
        print('Converted Drug column to binary')
        basmi_df['Drug'] = basmi_df['Drug'].notnull()

    no_drugs_dfs = []
    drugs_dfs = []
    i = 0
    for patient_id, patient_df in basmi_df.groupby('patient_id'):

        # if we don't have demographic info, skip this patient
        if patient_id not in demo_df.index.values:
            continue

        

        no_drugs_df = patient_df[~patient_df['Drug']]
        drugs_df = patient_df[patient_df['Drug']]

        if print_:
            print(patient_id,'\n')
            print('No Drugs:')
            print(no_drugs_df)
            print('\nDrugs:')
            print(drugs_df)
            print('\n\n')

        # Start date of periods for which patient took biologics
        drugs_dates = drugs_df.index.get_level_values('Date')
        no_drugs_dates = no_drugs_df.index.get_level_values('Date')

        drugs_start = min(drugs_dates) if not drugs_dates.empty else None
        no_drugs_start = min(no_drugs_dates) if not no_drugs_dates.empty else None

        # if a patient used drugs and then stopped, skip this patient
        if drugs_start and no_drugs_start and drugs_start < no_drugs_start:
            print('patient {} had invalid data'.format(patient_id))
            continue

        # If patient had taken drugs, save the data
        if not drugs_df.empty:
            drugs_dfs.append(drugs_df)

        # If patient had data for when not taking drugs, save the data
        if not no_drugs_df.empty:
            no_drugs_dfs.append(no_drugs_df)

        # Circuit breaker
        if break_at and i == break_at:
                break

        i += 1

    no_drugs_df = pd.concat(no_drugs_dfs)
    drugs_df = pd.concat(drugs_dfs)
    
    return drugs_df, no_drugs_df