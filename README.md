# enjoy-workflow

## Conda Environment

```bash
conda create -n enjoy-workflow python==3.12
conda activate enjoy-workflow
```

## Requirement

```bash
pip install jupyterlab kafka-python==2.3.0 apache-airflow==3.1.5 -r requirements/requirements.txt
```

## AWS EC SSH & MSK SSH Tunnel

```bash
ssh -i <PEM.FILE> ec2-user@<IP>
```

```bash
ssh -i ~/.ssh/prod-dataengineer-ec2.pem -N \
-L <PORT>:<HOST>:<PORT> \
ec2-user@<IP>
```