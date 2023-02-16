# Trabajo Realizado por: Eduardo Salgado Díaz del Río
# Asignatura: Extracción, Tranformación y Carga 


# Objetivo final de la practica:
# Las dos tablas finales que crearás y subirás a la base de datos serán las siguientes:
# • Tabla 1. Evolución mensual del número de críticas por distrito,
#   con predicción para el mes siguiente.
# • Tabla 2. Distribución del tipo de alojamiento room_type por distrito. Incluye:
#  – Notamediaponderada(review_scores_ratingponderadoconnumber_of_reviews).
#  – Precio mediano (price).
#  – Número de alojamientos (id).

# Práctica AIRBNB, en R:

# Primero cargamos las librerías
library(dplyr)
library(tidyverse)
library(stringr)

# Segundo, cargamos la base de datos mediante una conexión 
conn <- DBI::dbConnect(RSQLite::SQLite(), "airbnb.sqlite")

#Imprimimos las tablas para ver los datos de las tablas y como podemos unirlas 
Hoods <- tbl(conn, sql("SELECT * FROM Hoods LIMIT 5"))
Listings <- tbl(conn, sql("SELECT * FROM Listings LIMIT 5"))
colnames(Listings) # Puesto que no sabemos el nombre de las columnas
Reviews <- tbl(conn, sql("SELECT * FROM Reviews LIMIT 5"))


# Primera parte del proyecto: Extracción(Listings)
query = "SELECT t1.price, t1.number_of_reviews, t1.room_type, 
t1.review_scores_rating, t2.neighbourhood_group as district
FROM Listings as t1
INNER JOIN Hoods as t2 ON t1.neighbourhood_cleansed = t2.neighbourhood"

# En la primera query de SQL, unimos las tablas Hoods y listenings mediante 
# Listings.neighbourhood_cleansed y Hood.neighbourhood

# Descargamos el DataFrame
listings <- collect(tbl(conn, sql(query)))
glimpse(listings)

# 2. Extracción (reviews).

# Segunda query de SQL para hacer la extracción
second_query <- "SELECT COUNT(Reviews.id) as reviews_number,
Hoods.neighbourhood_group as district, strftime('%Y-%m', Reviews.date) as mes
FROM Reviews
INNER JOIN Listings
ON Listings.id = Reviews.listing_id
INNER JOIN Hoods
ON Listings.neighbourhood_cleansed=Hoods.neighbourhood
WHERE strftime('%Y', Reviews.date) NOT LIKE '2010'
GROUP BY district, mes"
#DataFrame con la información extraída
reviews <- collect(tbl(conn, sql(second_query)))
glimpse(reviews)

## 3. Transformación (listings).


listings$price <- gsub(pattern = "[,//$]", replacement = "", x = listings$price)
listings$price <- as.numeric(listings$price)
typeof(listings$price)


## 4. Transformación (listings).


# Cambiar NAs en number_of_reviews

# Para Number of Reviews
# Iteramos por las filas del df
# Obtenemos la columna de tipo de habitación y número de reseñas
room_type <- listings$room_type
number_of_reviews <- listings$number_of_reviews
mask <- is.na(number_of_reviews)

for (i in 1:length(mask)) {
  if (mask[i]) {
    rt <- room_type[i]
    number_of_reviews[i] <- sample(na.omit(number_of_reviews[room_type==rt]), size=1)
  }
}

listings$number_of_reviews <- number_of_reviews

# Realizamos la comprobacion de que no quedan NAs
sum(is.na(listings$number_of_reviews))

##################################################################
# Cambiar NAs en review_scores_rating
review_scores_rating <- listings$review_scores_rating

# Creamos una máscara para seleccionar los elementos NA en la columna de puntuación de reseñas
mask <- is.na(review_scores_rating)

# Iteramos por cada elemento de la máscara
for (i in 1:length(mask)) {
  # Si el elemento es NA, reemplazamos el valor por uno aleatorio correspondiente al tipo de habitación
  if (mask[i]) {
    rt <- room_type[i]
    review_scores_rating[i] <- sample(na.omit(review_scores_rating[room_type==rt]), size=1)
  }
}

# Asignamos la columna modificada al data frame
listings$review_scores_rating <- review_scores_rating
sum(is.na(listings$review_scores_rating))



## 5. Transformación (listings).

# Primera tabla de la práctica
tabla_list <- listings %>% 
  group_by(district,room_type) %>% 
  summarise(avg_review = weighted.mean(review_scores_rating,number_of_reviews),
            median_price = median(price))
tabla_list

# 6. Transformación (reviews).


### Hacemos las predicciones para el mes 2021-08 
reviews_prediction <- reviews %>% 
  select(everything()) %>%  # Seleccionamos todas las tablas de reviews
  filter(mes == '2021-07') %>%  # Filtramos por el mes de julio
  mutate(mes = '2021-08') # Creamos con el mes de agosto

# Unimos el dataframe con las predicciones con el dataframe original

reviews <- reviews %>%
  rbind(reviews_prediction) %>%
  arrange(district, mes)
# Si imprimimos la tabla review encontramos el objetivo de la práctica número 1 

reviews 

# 7.  Transformación (reviews).

#

reviews_mes <- tibble(expand.grid(district = unique(reviews$district), 
                         mes = substring(seq(as.Date("2011-01-01"),
                                             as.Date("2021-08-01"),
                                             by="months"), 1, 7))) 
# Hacemos un join para el dataframe anterior y el nuevo creado, rellenandose,
# asi los valores mensuales que faltan con un NA
reviews_pred <- reviews %>% 
  full_join(reviews_mes) %>% 
  arrange(district,mes) %>% 
  # Convertimos los NAs que acabamos de introducir en 0
  mutate(reviews_number = ifelse(is.na(reviews_number), 0, reviews_number))
reviews_pred

# 8. Carga.


#Subimos la tabla tabla_list a la base de datos
RSQLite::dbWriteTable(conn, "RoomTxdistric", tabla_list)
collect(tbl(conn, sql( "SELECT * FROM RoomTxdistric LIMIT 10")))

#Subimos la tabla reviews_pred a la base de datos


RSQLite::dbWriteTable(conn, "Reviews_pred", reviews_pred)
collect(tbl(conn, sql("SELECT * FROM Reviews_pred LIMIT 10")))

RSQLite::dbDisconnect(conn)


















