package main

import (
	"build_your_own_db/b-tree"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

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

// avec cette méthode résulut la problme de persiste de la data dnas le disk
// !!! ma ne pas la méthadata (un autre histoire ) pour cela why database are preferred over files for persistingdata to the disk

func LogCreate(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0664)
}

func logAppend(fp *os.File, line string) error {
	buf := []byte(line)
	buf = append(buf, '\n')
	_, err := fp.Write(buf)
	if err != nil {
		return err
	}
	return fp.Sync()
}

//maintenant on try to implemtner un B_tree et voire c'est quoi sa relation avec indexation et KV store

func main() {
	// Créer une nouvelle instance de test
	c := b_tree.NewC()

	// Test 1: Insertion simple et vérification
	fmt.Println("Test 1: Insertion simple")
	testBasicInsert(c)

	fmt.Println("\nTest 2: Test de suppression")
	testDeletion(c)

	// Test 5: Test de récupération
	fmt.Println("\nTest 5: Test de récupération")
	testRetrieval(c)

}

func testBasicInsert(c *b_tree.C) {
	defer c.Clear()

	// Insérer quelques valeurs
	c.Add("key1", "value1")
	c.Add("key2", "value2")
	c.Add("key3", "value3")

	if err := c.Verify(); err != nil {
		fmt.Printf("❌ Échec de la vérification après insertion: %v\n", err)
		return
	}

	if c.Size() != 3 {
		fmt.Printf("❌ Taille incorrecte. Attendu: 3, Obtenu: %d\n", c.Size())
		return
	}

	fmt.Println("✅ Test d'insertion réussi")
}

func testDeletion(c *b_tree.C) {
	defer c.Clear()

	// Préparer les données
	c.Add("key1", "value1")
	c.Add("key2", "value2")
	c.Add("key3", "value3")

	// Tester la suppression
	if !c.Del("key2") {
		fmt.Println("❌ Échec de la suppression de key2")
		return
	}

	// Vérifier que la clé a bien été supprimée
	if val, exists := c.Get("key2"); exists {
		fmt.Printf("❌ La clé supprimée existe toujours avec la valeur: %s\n", val)
		return
	}

	if err := c.Verify(); err != nil {
		fmt.Printf("❌ Échec de la vérification après suppression: %v\n", err)
		return
	}

	fmt.Println("✅ Test de suppression réussi")
}

func testRetrieval(c *b_tree.C) {
	defer c.Clear()

	// Préparer un grand jeu de données avec des clés de différentes tailles
	testData := map[string]string{
		"a":               "court",
		"clé_longue":      "valeur moyenne",
		"clé_très_longue": strings.Repeat("x", 100),
	}

	// Insérer les données
	for k, v := range testData {
		c.Add(k, v)
	}

	// Tester la récupération
	for k, expectedVal := range testData {
		val, exists := c.Get(k)
		if !exists {
			fmt.Printf("❌ Clé non trouvée: %s\n", k)
			return
		}
		if val != expectedVal {
			fmt.Printf("❌ Valeur incorrecte pour %s. Attendu: %s, Obtenu: %s\n",
				k, expectedVal, val)
			return
		}
	}

	// Tester une clé inexistante
	if _, exists := c.Get("clé_inexistante"); exists {
		fmt.Println("❌ Une clé inexistante a été trouvée")
		return
	}

	fmt.Println("✅ Test de récupération réussi")
}
