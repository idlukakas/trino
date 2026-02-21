import json
import re

# Your specific header
raw_header = '"id_Lead","dt_Criacao","dt_Modificacao","tp_Entrada","id_Usuario","id_Cliente","id_Funil","ds_Chave","id_Marca","id_Unidade","fl_Consultado","fl_Higienizado","dt_Higienizado","ds_Lead_Migrado","r1","r2","r3","r4","r5","r6","r7","r8","r9","r10","r11","r12","r13","r14","r15","r16","r17","r18","r19","r20","r21","r22","r23","r24","r25","r26","r27","r28","r29","r30","r31","r32","r33","r34","r35","r36","r37","r38","r39","r40","r41","r42","r43","r44","r45","r46","r47","r48","r49","r50","r51","r52","r53","r54","r55","r56","r57","r58","r59","r60","r61","r62","r63","r64","r65","r66","r67","r68","r69","r70","r71","r72","r73","r74","r75","r76","r77","r78","r79","r80","r81","r82","r83","r84","r85","r86","r87","r88","r89","r90","r91","r92","r93","r94","r95","r96","r97","r98","r99","r100","r101","r102","r103","r104","r105","r106","r107","r108","r109","r110","r111","r112","r113","r114","r115","r116","r117","r118","r119","r120","r121","r122","r123","r124","r125","r126","r127","r128","r129","r130","r131","r132","r133","r134","r135","r136","r137","r138","r139","r140","r141","r142","r143","r144","r145","r146","r147","r148","r149","r150","r151","r152","r153","r154","r155","r156","r157","r158","r159","r160"'

def generate_schema(header_text):
    # Clean the quotes and split by comma
    cols = [c.strip('"') for c in header_text.split(',')]
    schema = []

    for col in cols:
        name = col.strip()
        
        # Rule 1: Starts with 'dt' -> DATETIME
        if name.lower().startswith('dt'):
            data_type = "DATETIME"
        
        # Rule 2: Starts with 'id' -> STRING
        elif name.lower().startswith('id'):
            data_type = "STRING"
            
        # Rule 3: Starts with 'r' followed by digits OR everything else -> STRING
        # (Since you want both r1...r160 and general text as strings)
        else:
            data_type = "STRING"

        schema.append({
            "name": name,
            "type": data_type,
            "mode": "NULLABLE"
        })
    
    return schema

# Create the file
json_schema = generate_schema(raw_header)

with open('schema.json', 'w', encoding='utf-8') as f:
    json.dump(json_schema, f, indent=2)

print(f"Created schema.json with {len(json_schema)} columns.")
