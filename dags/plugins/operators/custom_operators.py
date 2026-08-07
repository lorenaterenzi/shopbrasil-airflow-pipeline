from airflow.exceptions import AirflowException

def validar_produtos_func(dataset: list) -> list:
    """Função para validar o schema dos produtos recebidos da API."""
    if not isinstance(dataset, list) or len(dataset) == 0:
        raise AirflowException("Dataset vazio ou formato inválido recebido da API.")
    
    colunas_obrigatorias = {'id', 'title', 'price', 'category'}
    for prod in dataset:
        if not colunas_obrigatorias.issubset(prod.keys()):
            raise AirflowException(f"Produto ID {prod.get('id')} não possui o schema válido.")
    
    return dataset