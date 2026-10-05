# data-eng-project-weather-crashes
Data Engineering course project at the University of Tartu: analyzing the relationship between weather conditions and traffic crashes.
## Datasets
For this project we use three datasets:
### 1) Weather data 2026 from Estonia form Ilmateenistus, Keskkonnaagentuur https://www.ilmateenistus.ee/kliima/ajaloolised-ilmaandmed/ 
- Data per one hour for every weather station, pulled once a day
- API pull as a .json file
### 2) Traffic accident data about the year 2026 from The Estonian Motor Insurance Bureau, https://kaart.lkf.ee/
- API pull as a .csv file
### 3) Weather station coordinates from the keskkonnaandmed API
- Pulled once to match coordinates to weather station code and name
- API request to this endpoint: https://keskkonnaandmed.envir.ee/f_kliima_jaam_vaatlus?select=jaam_kood,jaam_nimi,jaam_nimi_eng,pikkuskraad,laiuskraad,korgus_merepinnast_m,jaam_periood_algus,jaam_periood_lopp 

## Team members
* Triinu Vaher
* Laura Laugma
* Triine Kose
* Kaaren Tenson
* Raio Mitt
