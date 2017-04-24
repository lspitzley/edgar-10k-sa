
in_filename = 'gen_review_feature.csv'
out_filename = 'gen_review_feature2013-2016.csv'


fin = open(in_filename,'r')
fout = open(out_filename,'w')

next(fin) # Skip header

header = ['mda_file','2388']
fout.write(','.join(header) + '\n')
for line in fin.readlines():
    row = line.strip().split(',')
    
    parsed_row = [ row[0], row[2389] ] 
    
    fout.write(','.join(parsed_row) + '\n')

fin.close()
fout.close()
