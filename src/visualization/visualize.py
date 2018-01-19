def visualize_section():

	index_list = df_trecho_via.index.tolist()

	x_tick_labels = []
	for item in index_list:
	    inicio = item[1].split(" a ")[0]
	    fim = item[1].split(" a ")[1]
	    if inicio == "0":
	        tick_label = str(item[0]) + ":" + inicio + "0 a " + str(item[0]) + ":" + fim
	    else:
	        tick_label = str(item[0]) + ":" + inicio + " a " + str(item[0]) + ":" + fim
	    x_tick_labels.append(tick_label)
	    
	plt.figure(figsize=(12,6))
	ax = df_trecho_via["Velocidade Média (km/h)"].plot.bar()
	ax.spines['top'].set_visible(False)
	ax.spines['right'].set_visible(False)
	ax.spines['left'].set_visible(False)

	plt.ylabel('Velocidade Média (km/h)', fontsize=14)
	plt.xlabel('Intervalo de hora', fontsize=14)

	ax.set_xticklabels(x_tick_labels)
	plt.xticks(rotation=70)
	ttl = plt.title("Variação de Velocidade Média (km/h) no trecho " + str(trch_id) + " em horários de pico", fontsize=15, fontweight="bold")
	ttl.set_position([.5, 1.05])
	labels

	plt.show()