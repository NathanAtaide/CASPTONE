# `src/` — código Python compartilhado

## Papel na arquitetura

Guarda o código Python que é **reutilizado por mais de um componente**. Hoje contém um único
pacote, [`lakehouse/`](lakehouse/README.md), usado tanto pelas DAGs do Airflow quanto pelos
notebooks do Jupyter.

A ideia é que as DAGs fiquem pequenas (só orquestração) e a lógica de dados (como conectar, onde
gravar, como nomear) fique em um lugar só — assim pipeline e exploração se comportam igual.

## Como chega aos containers

A pasta é montada como volume **somente leitura** em `/opt/platform/src` nos containers do Airflow
e do Jupyter, e esse caminho está no `PYTHONPATH`. Logo:

```python
from lakehouse import connect, setting
```

funciona em qualquer task ou notebook, e **alterações no código valem imediatamente** (sem rebuild
da imagem). No Airflow, tasks novas já usam o código novo; em notebooks, reinicie o kernel.

## Para adicionar um pacote novo

Crie `src/<nome_do_pacote>/__init__.py` (com um `README.md` explicando o papel dele). Ele já fica
importável nos dois ambientes.
