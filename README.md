# DE skill test eFishery

## soal1-airflow

**Tentang Project**

ini adalah project DAG Airflow untuk melakukan load data dari db data operational ke db datawarehouse

diagram data operational: https://dbdiagram.io/d/6069e955ecb54e10c33e9fea
diagram data warehouse: https://dbdiagram.io/d/6069e955ecb54e10c33e9fea

Teknologi
* Python 3.9.7
* Docker 20.10.17
* Docker Compose 1.29.2
* Airflow 2.3.3

### Skema alternatif

diagram dwh alternatif: https://dbdiagram.io/d/62f7b133c2d9cf52fa9f0108

Perubahan
* menambahkan surrogate key untuk membantu menampung history tiap perubahan yg ada di dimension table
* menambah product sehingga bisa mendapatkan jumlah penjualan perproduct
