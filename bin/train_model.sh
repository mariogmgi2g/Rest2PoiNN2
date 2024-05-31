#!/bin/bash

nohup '/media/nas/mariog/miniconda3/envs/tf/bin/python3' -u -m main_entrenamiento_nn.py > "./data/nn/training results/entrenamiento.log" &
