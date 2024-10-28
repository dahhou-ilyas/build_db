package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	fmt.Println("Hello World")
}

func saveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	defer fp.Close()

	_, err = fp.Write(data)
	return err
}

/*
le fonctionne saveData1 il y a des inconveniet:
1. It truncates the file before updating it. What if the file needs to be read concurrently?
2. Writing data to files may not be atomic, depending on the size of the write. Con-
current readers might get incomplete data.
3. When is the data actually persisted to the disk? The data is probably still in the
operating system’s page cache after the write syscall returns. What’s the state of the
file when the system crashes and reboots?
*/
//------------------------------------------------------------------

//une autre approche mieux que la précedent (Atomic Renaming)

func randomInt() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(1000)
}
func saveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt()) // crée un nom aléatoire pour le ficher
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)

	if err != nil {
		os.Remove(tmp)
		return err
	}

	return os.Rename(tmp, path)

}

/*
AVANTAGE:
cette approche est mieux que la précedent car on repsect le principe de l'atomique
car on crée un ficher temporaire et aprée on finie tous dnas ce ficher temporarire on le renomé sur le ficher
sur le ficher que nous souhaite (donc si un erreur devien on supprime le ficher temp et le ficher originale rest safe sans ecrire dans lui un chose)
*/
//------------------------------------------------------------------------------------------------------------------------
/*
PAS ENCORE REGLÉ :
	meme avec cette solution ona encore des probléme car les ficher sont stocker dans le mémoire et ne conuue pas
	quand vous allez stocker dnas le disk (car avant que le ficher stocker dans le disk le os le stocker dans memory pour les perfermonce)

1 il n'est pas garanti que les données soient écrites sur le disque immédiatement après leur écriture dans le programme.
2 Les métadonnées, telles que la taille du fichier, peuvent être écrites sur le disque avant que les données elles-mêmes ne soient effectivement persistées. Si le système plante ou subit une panne de courant après que les métadonnées ont été écrites mais avant que les données soient enregistrées, cela peut créer une incohérence
3 L'exemple donné sur les fichiers journaux (log files) ayant des zéros après une panne de courant illustre ce problème.
*/

func saveData3(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt()) // crée un nom aléatoire pour le ficher
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fp.Write(data)
	if err != nil {
		os.Remove(tmp)
		return err
	}
	err = fp.Sync()
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}
