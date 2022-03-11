import pandas as pd
import re

# read the source data csv file
mydataset = pd.read_csv('airlines_2022.csv')

# duplicate the dataset for cleaning
mydataset_new = mydataset.copy()

# fill the blank values and replace "\n"/"\N" with Unknown
for column in mydataset_new:
	mydataset_new[column].fillna("Unknown", inplace = True)
for column in mydataset_new:
	for x in mydataset_new.index:
		found = mydataset_new.loc[x, column]
		if str(found).lower() == "\\n" or str(found).rstrip() == "":
			mydataset_new.loc[x, column] = "Unknown"

# create an empty dictionary to store the number of tuples removed
tuplesDict = {}

# get the number of data in the dataset and store it in sourceDataTuplesCount
sourceDataTuplesCount = len(mydataset_new.index)

# define a function to calculate the number of tuples removed
def tuplesRemoved(column, numberOfIndex, sourceDataTuplesCount):
	tuplesDict[column] = sourceDataTuplesCount - numberOfIndex
	sourceDataTuplesCount = numberOfIndex
	return sourceDataTuplesCount

# clean the dataset based on the Airline ID and Name attributes using regular expressions
for x in mydataset_new.index:
	if mydataset_new.loc[x, "Airline ID"] < 0:
		mydataset_new.loc[x, "Airline ID"] = 0
	name_search = mydataset_new.loc[x, "Name"]
	name_search_finding = re.findall("^[a-zA-Z0-9]", str(name_search))
	if len(name_search_finding) == 0:
		mydataset_new.drop(x, inplace = True)

# get the number of data in the dataset after cleaning and store it in sourceDataTuplesCount
sourceDataTuplesCount = tuplesRemoved("Name", len(mydataset_new.index), sourceDataTuplesCount)

# clean the dataset based on the Alias attribute using regular expressions twice
for x in mydataset_new.index:
	alias_search = mydataset_new.loc[x, "Alias"]
	alias_search_finding = re.findall("^[a-zA-Z0-9][a-zA-Z0-9\(\)\-&\.\s]*$", str(alias_search))
	if len(alias_search_finding) == 0:
		mydataset_new.drop(x, inplace = True)

# get the number of data in the dataset after cleaning and store it in sourceDataTuplesCount
sourceDataTuplesCount = tuplesRemoved("Alias", len(mydataset_new.index), sourceDataTuplesCount)

# clean the dataset based on the IATA attribute using regular expressions
for x in mydataset_new.index:
	iata_search = mydataset_new.loc[x, "IATA"]
	iata_search_finding = re.findall("[a-zA-Z0-9][a-zA-Z0-9]", str(iata_search))
	if len(iata_search_finding) == 0:
		mydataset_new.drop(x, inplace = True)

# get the number of data in the dataset after cleaning and store it in sourceDataTuplesCount
sourceDataTuplesCount = tuplesRemoved("IATA", len(mydataset_new.index), sourceDataTuplesCount)

# clean the dataset based on the ICAO attribute using regular expressions
for x in mydataset_new.index:
	icao_search = mydataset_new.loc[x, "ICAO"]
	icao_search_finding = re.findall("[a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9]", str(icao_search))
	if len(icao_search_finding) == 0:
		mydataset_new.drop(x, inplace = True)
	elif len(icao_search_finding) != 1 and icao_search != "Unknown":
		mydataset_new.drop(x, inplace = True)

# get the number of data in the dataset after cleaning and store it in sourceDataTuplesCount
sourceDataTuplesCount = tuplesRemoved("ICAO", len(mydataset_new.index), sourceDataTuplesCount)

# clean the dataset based on the Callsign attribute using regular expressions
for x in mydataset_new.index:
	callsign_search = mydataset_new.loc[x, "Callsign"]
	callsign_search_finding = re.findall("^[a-zA-Z0-9\s]*$", str(callsign_search)) #  ^\w{1}.+
	if len(callsign_search_finding) == 0:
		mydataset_new.drop(x, inplace = True)

# get the number of data in the dataset after cleaning and store it in sourceDataTuplesCount
sourceDataTuplesCount = tuplesRemoved("Callsign", len(mydataset_new.index), sourceDataTuplesCount)

# clean the dataset based on the Country attribute using regular expressions
for x in mydataset_new.index:
	country_search = mydataset_new.loc[x, "Country"]
	country_search_finding = re.findall("^[a-zA-Z0-9\s]*$", str(country_search))
	if len(country_search_finding) == 0:
		mydataset_new.drop(x, inplace = True)

# get the number of data in the dataset after cleaning and store it in sourceDataTuplesCount
sourceDataTuplesCount = tuplesRemoved("Country", len(mydataset_new.index), sourceDataTuplesCount)

# clean the dataset based on the Active attribute using regular expressions
for x in mydataset_new.index:
	active_search = mydataset_new.loc[x, "Active"]
	mydataset_new.loc[x, "Active"] = active_search.upper()

# get the number of data in the dataset after cleaning and store it in sourceDataTuplesCount
sourceDataTuplesCount = tuplesRemoved("Active", len(mydataset_new.index), sourceDataTuplesCount)

# remove duplicates to print out number of unique records in the source data
mydataset.drop_duplicates(inplace = True)
print("There are " + str(len(mydataset.index)) + " unique records included in the source data")

# print out number of unique values in each column of the dataset
for column in mydataset_new:
	uniqueCount = len(pd.unique(mydataset_new[column]))
	print("Number of unique values in column " + column + " : " + str(uniqueCount))

# print out number of "Unknown" values in each column of the dataset except for column Airline ID
for column in mydataset_new:
	if column == "Airline ID":
		continue
	UnknownCount = 0
	for x in mydataset_new.index:
		if mydataset_new.loc[x, column] == "Unknown":
			UnknownCount += 1
	print("Number of \"Unknown\" included in the " + column + " attribute after cleaning: " + str(UnknownCount))

# print out number of tuples removed based on each attribute
for column in mydataset_new:
	if column == "Airline ID":
		continue
	print("Number of tuples pending removal based on the " + column + " attribute: " + str(tuplesDict[column]))

# remove duplicates to print out number of unique tuples in the dataset after cleaning
mydataset_new.drop_duplicates(inplace = True)
print("There are " + str(len(mydataset_new.index)) + " unique tuples included in the cleaned dataset")

# calculate and print out percentage of tuples removed from the dataset after cleaning
tuplesRemovedPercentage = (len(mydataset.index) - len(mydataset_new.index)) / (len(mydataset.index)) * 100
print(str(round(tuplesRemovedPercentage, 2)) + "% of the tuples are removed from the dataset after cleaning")

# print out the attribute which caused the most tuples to be removed
maxAttribute = max(tuplesDict, key = tuplesDict.get)
print("Attribute " + maxAttribute + " caused the most tuples to be removed from the dataset")

# print out the Country which owns the most active operational routes and the number of routes for that country
print(str(mydataset_new["Country"].value_counts().idxmax()) + " owns the most active operational routes")
occurence = 0
for x in mydataset_new.index:
	occurence_search = mydataset_new.loc[x, ["Country", "Active"]]
	if occurence_search[0] == str(mydataset_new["Country"].value_counts().idxmax()) and occurence_search[1] == "Y":
		occurence += 1
print("There are " + str(occurence) + " active operational routes owned by " + str(mydataset_new["Country"].value_counts().idxmax()))

# extract the cleaned data into a new csv file
mydataset_new.to_csv("airlines_2022_cleaned.csv")