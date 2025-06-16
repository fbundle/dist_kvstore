package dist_kvstore

import "fmt"

func combineErrors(errs ...error) error {
	var errorList []error

	for _, err := range errs {
		if err == nil {
			continue
		}
		errorList = append(errorList, err)
	}
	if len(errorList) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errorList)
}
