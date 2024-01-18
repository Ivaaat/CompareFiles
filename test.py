import csv
# Lists containing column values for csv file
column1 = ['Ritika', 'Mark', 'Suse', 'Shaun']
column2 = [27, 28, 29, 30]
column3 = ['Delhi', 'Sydney', 'Las Vegas', 'London']
column4 = ['India', 'Australia', 'USA', 'UK']
# Open csv file for writing
with open('employees3.csv', 'w') as fileObj:
    # Create a CSV Writer object
    writerObj = csv.writer(fileObj)
    # Zip all the column lists and iterate over zipped objects
    for row in zip(column1, column2, column3, column4):
        # Add a zipped object as a row in the csv file
        writerObj.writerow(row)