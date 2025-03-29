import json
import re
import sys

import requests
from bs4 import BeautifulSoup

ELECTIONS = {
    "242": "2023 Municipal Runoff - 4/3/23",
    "241": "2023 Municipal General - 2/28/23",
    "156": "2022 General Election - 11/8/2022",
    "252": "2022 Primary - Democratic - 6/28/2022",
    "253": "2022 Primary - Republican - 6/28/2022",
    "254": "2022 Primary - Libertarian - 6/28/2022",
    "255": "2022 Primary - Non-Partisan - 6/28/2022",
    "251": "2020 General Election - 11/3/2020",
    "250": "2020 Primary - Non-Partisan - 3/17/2020",
    "240": "2020 Primary - Republican - 3/17/2020",
    "230": "2020 Primary - Democratic - 3/17/2020",
    "220": "2019 Municipal Runoffs - 4/2/2019",
    "210": "2019 Municipal General - 2/26/2019",
    "200": "2018 General Election - 11/6/2018",
    "2": "2018 Primary - Non-Partisan - 3/20/2018",
    "1": "2018 Primary - Republican - 3/20/2018",
    "0": "2018 Primary - Democratic - 3/20/2018",
    "3": "2017 Municipal General - 2/28/2017",
    "4": "2016 General Election - 11/8/2016",
    "8": "2016 Primary - Non-Partisan - 3/15/2016",
    "7": "2016 Primary - Green - 3/15/2016",
    "6": "2016 Primary - Republican - 3/15/2016",
    "5": "2016 Primary - Democratic - 3/15/2016",
    "9": "2015 Municipal Runoffs - 4/7/2015",
    "10": "2015 Municipal General - 2/24/2015",
    "11": "2014 General Election - 11/4/2014",
    "12": "2014 Primary - Democratic - 3/18/2014",
    "13": "2014 Primary - Republican - 3/18/2014",
    "14": "2014 Primary - Green - 3/18/2014",
    "15": "2014 Primary - Non-Partisan - 3/18/2014",
    "16": "2013 Special Election - 2nd Congressional - 4/9/2013",
    "17": "2013 Special Primary - 2nd Congressional - Democratic - 2/26/2013",
    "18": "2013 Special Priamry - 2nd Congressional - Republican - 2/26/2013",
    "19": "2012 General Election - 11/6/2012",
    "20": "2012 Primary - Democratic - 3/20/2012",
    "21": "2012 Primary - Republican - 3/20/2012",
    "22": "2012 Primary - Green - 3/20/2012",
    "23": "2012 Primary - Non-Partisan - 3/20/2012",
    "24": "2011 Municipal Runoffs - 4/5/2011",
    "25": "2011 Municipal General - 2/22/2011",
    "26": "2010 General Election - 11/2/2010",
    "27": "2010 Primary - Democratic - 2/2/2010",
    "29": "2010 Primary - Republican - 2/2/2010",
    "31": "2010 Primary - Green - 2/2/2010",
    "33": "2009 Special Election - 5th Congressional - 4/7/2009",
    "34": "2009 Special Primary - 5th Congressional - Democratic - 3/3/2009",
    "36": "2009 Special Primary - 5th Congressional - Republican - 3/3/2009",
    "38": "2009 Special Primary - 5th Congressional - Green - 3/3/2009",
    "40": "2008 General Election - 11/4/2008",
    "45": "2008 Primary - Democratic - 2/4/2008",
    "50": "2008 Primary - Republican - 2/4/2008",
    "55": "2008 Primary - Green - 2/4/2008",
    "60": "2007 Municipal Runoffs - 4/17/2007",
    "65": "2007 Municipal General - 2/27/2007",
    "70": "2006 General Election - 11/7/2006",
    "75": "2006 Primary - Democratic - 3/21/2006",
    "80": "2006 Primary - Republican - 3/21/2006",
    "85": "2006 Primary - Other - 3/21/2006",
    "90": "2004 General Election - 11/2/2004",
    "95": "2004 Primary - Democratic - 3/16/2004",
    "100": "2004 Primary - Republican - 3/16/2004",
    "101": "2004 Primary - Other - 3/16/2004",
    "105": "2003 Municipal Runoffs - 4/1/2003",
    "110": "2003 Municipal General - 2/25/2003",
    "115": "2002 General Election - 11/5/2002",
    "116": "2002 Primary - Democratic - 3/19/2002",
    "117": "2002 Primary - Republican - 3/19/2002",
    "118": "2002 Primary - Other - 3/19/2002",
    "120": "2000 General Election - 11/7/2000",
    "124": "2000 Primary - Democratic - 3/21/2000",
    "125": "2000 Primary - Republican - 3/21/2000",
}

MANUAL_ELECTIONS = {
    "19830": {
        "year": 1983,
        "date": "2/22/1983",
        "label": "1983 Primary - Democratic",
        "races": {"0": "Mayor"},
    },
    "19831": {
        "year": 1983,
        "date": "4/12/1983",
        "label": "1983 General Election",
        "races": {"0": "Mayor"},
    },
}


def election_name(name):
    date_str = name.split(" ")[-1]
    return name.replace(f" - {date_str}", "")


def race_name(label):
    return re.sub(r"- (REP|DEM|LIB)$", "", label)


def get_races(soup):
    return {
        o.get("value").strip() or "0": race_name(o.get_text().split("\n")[0].strip())
        for o in soup.select("#race option")
    }


if __name__ == "__main__":
    election_metadata = {
        k: {
            "year": int(v.split(" ")[0]),
            "date": v.split(" ")[-1],
            "label": election_name(v),
            "races": {},
        }
        for k, v in ELECTIONS.items()
    }
    for election in ELECTIONS.keys():
        res = requests.get(
            f"https://chicagoelections.gov/en/election-results.asp?election={election}"
        )
        election_metadata[election]["races"] = get_races(
            BeautifulSoup(res.text, features="lxml")
        )
    election_metadata = {**election_metadata, **MANUAL_ELECTIONS}
    json.dump(election_metadata, sys.stdout)
