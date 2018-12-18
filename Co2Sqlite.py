# C02 data till SQLITE
import csv
import sqlite3

csv_file = 'c:/Users/seas19062/Downloads/co2data.csv'
database_file = 'c:/Users/seas19062/co2.db'

# current_countries


def main():
    print("Inserting file : ", csv_file)
    parse_database()
    parse_csv()


def parse_database():
    global current_countries
    global current_sectors

    sql = "SELECT Code, CountryName from Countries"
    conn = sqlite3.connect(database_file)
    cur = conn.cursor()
    cur.execute(sql)
    current_countries = cur.fetchall()

    sql = "SELECT SectorName from Sector"
    cur.execute(sql)
    current_sectors = cur.fetchall()

    conn.close()


def add_countries_and_sectors(countries, sectors):
    conn = sqlite3.connect(database_file)
    cur = conn.cursor()
    for y in countries:
        code = y[0]
        country = y[1]
        cur.execute('INSERT INTO Countries (Code, CountryName) VALUES (?,?);', (code, country, ))

    for y in sectors:
        sector = y
        cur.execute('INSERT INTO Sector(SectorName) VALUES (?);', (sector, ))

    conn.commit()
    conn.close()


def add_co2_data(data_to_add):
    conn = sqlite3.connect(database_file)
    cur = conn.cursor()

    for co2Item in data_to_add:
        cur.execute('INSERT INTO Co2Data (ISO_CODE, Sector, Value, Year) VALUES (?,?,?,?);',
                    (co2Item.iso_code, co2Item.sector, co2Item.value, co2Item.year, ))

    conn.commit()
    conn.close()


def parse_csv():
    begin_read = False
    rowdata = 4
    countries_to_add = [] # skapa tuple
    sectors_to_add = []
    data_to_add = []
    year_array = []
    with open(csv_file) as data_csv_file:
        read_csv = csv.reader(data_csv_file, delimiter=',')
        line_count = 0
        for row in read_csv:
            line_count += 1

            if str(row[0]).__contains__("ISO_CODE"):
                begin_read = True
                print("Begin read. line_count is now", str(line_count))
                while rowdata < (len(row)):
                   year_array.append(row[rowdata])
                   rowdata += 1

                rowdata=4

            if begin_read and not str(row[0]).__contains__("ISO_CODE"):
                iso_code = row[0]
                country_name = row[1]
                if not [item for item in current_countries if item[0] == iso_code]:
                    if not [item for item in countries_to_add if item[0] == iso_code]:
                        countries_to_add.append(tuple((iso_code, country_name)))

                sector = row[2]
                if not any(sector in s for s in current_sectors):
                    if not any(sector in s for s in sectors_to_add):
                        sectors_to_add.append(sector)

                # parse actual data

                while rowdata < (len(row)):
                    co2_value = (row[rowdata])
                    year_value = year_array[rowdata -4]

                    data_object = Co2Data(iso_code, sector, co2_value, year_value)
                    data_to_add.append(data_object)

                    rowdata += 1
                rowdata = 4

    data_csv_file.close()
    print("File contains > ", str(line_count) + " rows")

    add_countries_and_sectors(countries_to_add, sectors_to_add)
    print("Done adding countries and sectors.")

    add_co2_data(data_to_add)


class Co2Data:
    def __init__(self, iso_code, sector, value, year):
        self.iso_code = iso_code
        self.sector = sector
        self.value = value
        self.year = year


if __name__ == '__main__':
    main()
