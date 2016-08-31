#!/usr/bin/env bash
python3 -c 'import tensorflow as tf;sess = tf.Session()' 2>&1 | grep /gpu: | awk '{print $6; s = ""; for (i = 8; i <= NF; i++) s = s $i " "; print s}'