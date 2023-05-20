# ETL projekt Eesti välisõhu andmete põhjal
## Mõõdetavad parameetrid:
| Attr  | example value | unit    | Description                 |
| ----- | ------------- | ------- | --------------------------- |
| SO2   | 0,23          | µg/m³ | Vääveldioksiid            |
| NO2   | 0,02          | µg/m³ | Lämmastikdioksiid          |
| CO    | 0,24          | mg/m³  | Süsinikoksiid              |
| O3    | 70,05         | µg/m³ | Osoon                       |
| PM10  | 8,55          | µg/m³ | Peened osakesed             |
| PM2.5 | 4,72          | µg/m³ | Eriti peened osakesed       |
| TEMP  | 9,72          | C       | Temperatuur                 |
| WD10  | 204,40        | deg     | Tuule suund 10 m kõrgusel  |
| WS10  | 1,56          | m/s     | Tuule kiirus 10 m kõrgusel |

## Extract
- [Eesti välisõhu kvaliteedi](https://airviro.klab.ee/) seire kodulehelt laetakse alla info .cvs failide kujul. 
- Andmed on Tartu seirejaamast ja hõlmavad 2022 aastat.

## Transform
- Kasutatakse projekti tarbeks loodud Openrefine tarkvara API funktsioone.
- Luuakse uus projekt.
- Imporditakse andmed cvs failist.
- Laetakse salvestatud operatsioonide json fail.
- Muudetakse tulpade andmetüüpe.
- Eksporditakse andmed ajutisse csv faili.

## Load
- Luuakse SQLite andmebaas.
- Imporditakse andmed csv failist.
- Luuakse uued tabelid: päevakeskmine ja kuukeskmine.
- Arvutakse keskmised väärtused ja lisatakse tabelitesse.

## Kasutamine
- API kasutamiseks on vajalik eelnevalt käivitada Openrefine tarkvara.
- Käivitada ETL.py fail (ETL.py ja openrefine_api.py failid peavad asuma samas kataloogis)
- ETL protsess peaks toimima automaatselt.
- Testitud Windowsis ja Linuxis


