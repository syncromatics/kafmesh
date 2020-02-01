package cmd

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/syncromatics/kafmesh/pkg/generator"

	"github.com/syncromatics/kafmesh/pkg/models"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "kafmesh-gen",
	Short: "kafmesh-gen is a code generator for kafmesh services",
	Long:  `A generator for kafmesh services`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			log.Fatal("wrong number of args. Should just have path to service config yaml")
		}

		servicePath := args[0]

		exPath, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}

		fullServicePath := filepath.Join(exPath, servicePath)

		serviceFile, err := os.Open(fullServicePath)
		if err != nil {
			log.Fatal(err)
		}
		defer serviceFile.Close()

		service, err := models.ParseService(serviceFile)
		if err != nil {
			log.Fatal(err)
		}

		exPath = filepath.Dir(fullServicePath)

		components := []*models.Component{}

		for _, g := range service.Components {
			cs, err := filepath.Glob(filepath.Join(exPath, g))
			if err != nil {
				log.Fatal(err)
			}
			for _, c := range cs {
				componentFile, err := os.Open(c)
				if err != nil {
					log.Fatal(err)
				}
				defer componentFile.Close()

				component, err := models.ParseComponent(componentFile)
				if err != nil {
					log.Fatal(err)
				}

				components = append(components, component)
			}
		}

		err = generator.Generate(generator.Options{
			RootPath:   exPath,
			Service:    service,
			Components: components,
		})
	},
}

// Execute the root command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
