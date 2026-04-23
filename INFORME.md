# TP Coordinación — Informe

## Descripción del sistema

El sistema procesa pedidos de frutas de múltiples clientes concurrentemente. Cada cliente envía registros `(fruta, cantidad)` a un gateway, que los distribuye a través de un pipeline distribuido compuesto por tres etapas: **Sum**, **Aggregation** y **Join**. El resultado es el top N de frutas con mayor cantidad total pedida por ese cliente.


## Identificación de clientes (`client_id`)

El Gateway genera un UUID por cada cliente que se conecta y lo envía en todos los mensajes internos. Cada componente del pipeline mantiene su estado en un diccionario `{client_id: ...}`.


## Patrón de coordinación usado
Para coordinar las diferentes etapas del pipeline elegí usar el patrón de coordinación Map - Reduce. 

### Etapa de Map - Sum 

Pensé a las instancias de los workers de Sum como la etapa del Map, en donde cada fruta va a un Sum worker que acumula la cantidad de fruta por cliente en un diccionario de la estructura `{client_id: {fruta: FruitItem}}`.

Para garantizar que cada SUm worker procese un solo mensaje a la vez, se configura el parámetro `prefetch_count=1` en el channel y con esto RabbitMQ solo manda el próximo mensaje cuando el worker actual confirma que el mensaje anterior recibido fue procesado. 
Con esto garantizamos que los mensajes de procesen de a uno.


#### Coordinación del EOF entre réplicas de Sum

El EOF de un cliente llega al `input_queue` compartido, por lo que lo recibe un único Sum worker. Ese worker necesita avisarle al resto de las instancias para que cada uno envíe sus parciales acumulados.

La idea original era enviar copias del EOF a la queue compartida (una para cada worker) pero lo que pasó es que no había garantía de que todos los workers Sum reciban la notificación de EOF, podía pasar que una misma instancia de SUm reciba 2 veces un EOF y otra instancia no reciba nada, entonces, terminaba mandando EOF vacios al Agg y los resultados no eran correctos.

Para evitar esta race condition lo que implementé es una EOF queue `{SUM_PREFIX}_{i}_eof` por cada instancia de Sum. El worker Sum que recibe el EOF original hace broadcast al resto enviando una copia a cada EOF queue. Como cada una de estas EOF queue tiene un solo consumidor me garantizo que todos los workers Sum estén notificados una sola vez.

Cada worker Sum consume de dos queues en simultáneo: del `input_queue` compartido por donde le llegan los datos, y de su propio EOF queue.

Para poder escuchar ambas queues en paralelo sin compartir recursos del middleware (la implementación original compartia recursos del middleware), cada queue tiene su propio thread. La `eof_input_queue` corre en un thread separado. Cuando llega un EOF, ese thread no toca el estado compartido directamente sino que schedulea el procesamiento en el event loop del thread principal usando `add_callback_threadsafe`, y espera a que termine antes de ackear. De esta forma, tanto el procesamiento de datos como el flush del EOF ocurren secuencialmente en el mismo event loop, sin necesidad de locks.


### Etapa Reduce parcial - Aggregation

Cada Sum envía sus parciales a un único Aggregation determinado por una función de hash, que suma todos los valores ASCII de cada caracter de la palabra correspondiente a cada fruta y hace un módulo por la variable `AGGREGATION_AMOUNT`

```
hash_name = sum(ord(c) for c in fruta) % AGGREGATION_AMOUNT
```

Esta función es determinística: el mismo nombre de fruta siempre produce el mismo `hash_name` en todos los procesos. Con esto garantizo que todas las sumas parciales de la misma fruta (que vengan de distintos Sum workers) siempre van al mismo Aggregation, que acumula el total.

El EOF sí se envía a **todos** los Aggregation workers. Cada Aggregation implementa una barrrera por conteo de EOF: espera recibir `SUM_AMOUNT` EOFs (uno por cada instancia de Sum) antes de calcular su top parcial. Recién cuando llega el último EOF sabe que tiene todos los datos de esa fruta para ese cliente.


### Etapa Join — Reduce global

Join implementa un segundo barrera por conteo de tops: espera recibir `AGGREGATION_AMOUNT` tops parciales por cliente. Cuando llegan todos, los joinea.


Cada Aggregation aporta el top de sus frutas. Join combina todos, reordena globalmente y toma los mejores `TOP_SIZE`. El resultado es el top final correcto.

## Mejoras respecto a la implementación inicial

La implementación original registraba ambos consumers (`input_queue` y `sum_{ID}_eof`) en el mismo channel, accediendo a `input_queue.channel` directamente desde fuera del middleware. Esto acoplaba la solución a RabbitMQ y rompía la independencia entre abstracciones.

La versión actual resuelve esto dando a cada queue su propia conexión y corriendo la `eof_input_queue` en un thread separado. Cuando llega un EOF, el EOF thread lo schedula en el event loop del main thread  con `add_callback_threadsafe` y espera a que termine de procesar todo. Así el procesamiento de datos y el flush del EOF son secuenciales en el mismo event loop, sin locks y sin compartir recursos internos del middleware entre abstracciones.

