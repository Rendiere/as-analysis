import pandas as pd
import config as cfg

def download_data():
    # TODO
    pass


def process_data():
    f_normed = f'{cfg.DATA_DIR}/bs_normed_full.xls'
    f_normed_agg = f'{cfg.DATA_DIR}/bs_normed_agg.xls'

    basmi_df = pd.read_excel(f'{cfg.DATA_DIR}/clean_basmi.xls', index_col=(0, 1))

    normed_df = normalize_timeline(basmi_df)
    normed_df.to_excel(f_normed)

    # Aggregate the normalized bs data by year and save to disk
    agg_normed_df = (normed_df.groupby('norm_years')
                     .agg({'BS': 'mean', 'norm_years': len})
                     .rename(columns={'norm_years': 'count'}).round(2)
                     )

    agg_normed_df.to_excel(f_normed_agg)


def normalize_timeline(basmi_df):
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

    # Turn the Drug column into binary
    basmi_df['Drug_Indicator'] = basmi_df['Drug'].notnull().map({False: 0, True: 1})
    basmi_df.drop('Drug', axis=1, inplace=True)

    # Sub-select BS score
    normed_bs_df = basmi_df.copy()
    normed_bs_df['norm_years'] = normed_bs_df.groupby(level=0)['BS'].transform(get_norm_years)

    # Bin data per year for each patient
    normed_bs_df = normed_bs_df.groupby(['patient_id', 'norm_years']).agg({'BS': 'mean'}).reset_index(level=1)
    # Round floats to 2 digits
    normed_bs_df = normed_bs_df.round(2)

    # Fix the missing data
    normed_bs_df = impute_missing_values(normed_bs_df)

    return normed_bs_df
