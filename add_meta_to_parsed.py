import pandas as pd

def run_merging( parsed_file, meta_file, result_file ):
	# Read CSV to pandas dataframes
	parsed_df = pd.read_csv(parsed_file,delimiter=',')
	meta_df = pd.read_csv(meta_file,
				delimiter='\t',
				names=['filename','date','origin_url','company_cik'])
	# Merge CSV according to filenames
	merged_df = parsed_df.merge(meta_df, on='filename')

	# Split Company Name & CIK code
	company_cik_list = merged_df['company_cik'].tolist()
	company_list, cik_list = split_company_cik(company_cik_list)

	company_df = pd.DataFrame(data=company_list,
				columns=['company'])
	cik_df = pd.DataFrame(data=cik_list,
				columns=['CIK'])

	# Remove original company with cik column
	merged_df.pop('company_cik')

	# Add company and cik to original merged dataframe
	frames = [ merged_df, company_df, cik_df ]
	result = pd.concat( frames, axis=1)

	# Write result to csv file
	result.to_csv( result_file, index=False )


def split_company_cik( company_with_cik_list ):
	company_names = []
	company_codes = []
	for company_cik in company_with_cik_list:
		split_comp = company_cik.split()
		name = ' '.join(split_comp[:-1])
		code = split_comp[-1]
		company_names.append(name)
		company_codes.append(code)
	return company_names, company_codes

if __name__ == "__main__":
	parsed_file = './Parser.csv'
	meta_file = './data/meta/all.meta.txt'
	result_file = './result/result.csv'

	run_merging( parsed_file, meta_file, result_file )
