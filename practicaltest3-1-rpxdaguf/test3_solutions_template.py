from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':

	# Definition of common variables
	filename = "file:///home/prac/test3/input/HotelReservations.csv"
	file = open("/home/prac/test3/output/result.txt",'w')

	# Define SparkContext and SparkSession
	sc = SparkContext(appName = "Test3")
	spark = SparkSession(sc)


	###Q1 - Load data, convert to dataframe, apply appropriate column names and variable types

	# Your solution goes here


	###Q2 - Determine the proportion of each market segment type

	# Your solution goes here
	
	# Printing the solution to the results.txt file
	file.write("Online = " +  + "%, Offline = " +  + "%, Corporate = " +  + "%, Complementary = " +  + "%, Aviation = " +  + "%\n\n")


	###Q3 - Which month has the least average lead time

	# Your solution goes here
	
	# Printing the solution to the results.txt file
	file.write("Month " +  + " has the least average lead time: " +  + " days.\n\n")


	###Q4 - For each room type, determine the average room price per night

	# Your solution goes here
	
	# Printing the solution to the results.txt file
	file.write("Average room prices: Room_Type 1 = $" +  + ", Room_Type 2 = $" +  + ", Room_Type 3 = $" +  + ", Room_Type 4 = $" +  + ", Room_Type 5 = $" +  + ", Room_Type 6 = $" +  + ", Room_Type 7 = $" +  + "\n\n")


	###Q5 - What is the longest lead time among customers who booked with children in 2018

	# Your solution goes here
	
	# Printing the solution to the results.txt file
	file.write("The longest lead time with children in 2018 is " +  + " days.")


    sc.stop()
	file.close()
