/*
SPDX-License-Identifier: Apache-2.0

Copyright Contributors to the Submariner project.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package federate

import "context"

type operationKeyType struct{}

// IsCreateContext returns true if the context indicates the resource is known to be new (create operation),
// allowing federators to skip existence checks and attempt a direct Create.
func IsCreateContext(ctx context.Context) bool {
	v, _ := ctx.Value(operationKeyType{}).(bool)
	return v
}

// WithCreateContext returns a context that signals to the federator that this is a create operation,
// so the expensive existence check (List-by-label) can be skipped.
func WithCreateContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, operationKeyType{}, true)
}
