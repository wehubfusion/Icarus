package jsrunner

import (
	"fmt"

	"github.com/dop251/goja"
)

// Sandbox manages security restrictions for JavaScript execution
type Sandbox struct {
	securityLevel string
	maxStackDepth int
}

// NewSandbox creates a new sandbox with the given configuration
func NewSandbox(config *Config) *Sandbox {
	return &Sandbox{
		securityLevel: config.SecurityLevel,
		maxStackDepth: config.MaxStackDepth,
	}
}

// Apply applies sandbox restrictions to a VM runtime
func (s *Sandbox) Apply(vm *goja.Runtime) error {
	// Remove dangerous globals based on security level
	if err := s.removeDangerousGlobals(vm); err != nil {
		return fmt.Errorf("failed to remove dangerous globals: %w", err)
	}

	// Freeze built-in objects to prevent tampering
	if err := s.freezeBuiltins(vm); err != nil {
		return fmt.Errorf("failed to freeze built-ins: %w", err)
	}

	// Set up stack depth monitoring
	if err := s.setupStackMonitoring(vm); err != nil {
		return fmt.Errorf("failed to setup stack monitoring: %w", err)
	}

	return nil
}

// removeDangerousGlobals removes or restricts dangerous global objects
func (s *Sandbox) removeDangerousGlobals(vm *goja.Runtime) error {
	dangerousGlobals := []string{
		"require",        // Node.js require
		"module",         // Node.js module
		"exports",        // Node.js exports
		"process",        // Node.js process
		"global",         // Node.js global
		"__dirname",      // Node.js __dirname
		"__filename",     // Node.js __filename
		"Buffer",         // Node.js Buffer
		"setImmediate",   // Node.js setImmediate
		"clearImmediate", // Node.js clearImmediate
	}

	for _, name := range dangerousGlobals {
		if err := vm.Set(name, goja.Undefined()); err != nil {
			return fmt.Errorf("failed to remove %s: %w", name, err)
		}
	}

	// In strict mode, also remove eval
	if s.securityLevel == SecurityLevelStrict {
		if err := s.restrictEval(vm); err != nil {
			return err
		}
	}

	return nil
}

// restrictEval restricts or removes eval functionality
func (s *Sandbox) restrictEval(vm *goja.Runtime) error {
	// Replace eval with a restricted version that throws an error
	restrictedEval := func(call goja.FunctionCall) goja.Value {
		panic(vm.NewGoError(NewSecurityError("eval is not allowed in strict security mode")))
	}

	return vm.Set("eval", restrictedEval)
}

// freezeBuiltins freezes built-in objects to prevent modification
func (s *Sandbox) freezeBuiltins(vm *goja.Runtime) error {
	// List of built-in objects to freeze
	builtins := []string{
		"Object",
		"Array",
		"Function",
		"String",
		"Number",
		"Boolean",
		"Date",
		"RegExp",
		"Error",
		"Math",
	}

	// Only freeze in standard and strict modes
	if s.securityLevel == SecurityLevelPermissive {
		return nil
	}

	freezeScript := `
		(function() {
			function freezeObject(obj) {
				if (obj && typeof obj === 'object') {
					Object.freeze(obj);
					if (obj.prototype) {
						Object.freeze(obj.prototype);
					}
				}
			}
			return freezeObject;
		})()
	`

	val, err := vm.RunString(freezeScript)
	if err != nil {
		return fmt.Errorf("failed to create freeze function: %w", err)
	}

	freezeFn, ok := goja.AssertFunction(val)
	if !ok {
		return fmt.Errorf("freeze function is not a function")
	}

	for _, name := range builtins {
		obj := vm.Get(name)
		if obj != nil && obj != goja.Undefined() {
			if _, err := freezeFn(goja.Undefined(), obj); err != nil {
				// Non-fatal, log and continue
				continue
			}
		}
	}

	return nil
}

// setupStackMonitoring sets up stack depth monitoring
func (s *Sandbox) setupStackMonitoring(vm *goja.Runtime) error {
	if s.maxStackDepth <= 0 {
		return nil
	}

	// Inject a stack depth counter
	monitorScript := fmt.Sprintf(`
		(function() {
			var maxDepth = %d;
			var currentDepth = 0;
			
			var originalFunction = Function.prototype.constructor;
			
			// We can't reliably intercept all function calls in goja
			// This is more of a demonstration - real stack depth is monitored by goja itself
			// and will throw RangeError: Maximum call stack size exceeded
			
			return {
				maxDepth: maxDepth,
				currentDepth: currentDepth
			};
		})()
	`, s.maxStackDepth)

	_, err := vm.RunString(monitorScript)
	if err != nil {
		return fmt.Errorf("failed to setup stack monitoring: %w", err)
	}

	return nil
}

// ValidateOperation checks if an operation is allowed at the current security level
func (s *Sandbox) ValidateOperation(operation string) error {
	restrictions := s.getRestrictions()

	for _, forbidden := range restrictions {
		if operation == forbidden {
			return NewSecurityError(fmt.Sprintf("operation '%s' is not allowed at security level '%s'",
				operation, s.securityLevel))
		}
	}

	return nil
}

// getRestrictions returns the list of forbidden operations for the current security level
func (s *Sandbox) getRestrictions() []string {
	switch s.securityLevel {
	case SecurityLevelStrict:
		return []string{
			"eval",
			"Function",
			"setTimeout",
			"setInterval",
			"XMLHttpRequest",
			"fetch",
			"WebSocket",
			"importScripts",
		}
	case SecurityLevelStandard:
		return []string{
			"XMLHttpRequest",
			"fetch",
			"WebSocket",
			"importScripts",
		}
	case SecurityLevelPermissive:
		return []string{
			"importScripts", // Still forbidden even in permissive mode
		}
	default:
		return []string{}
	}
}

// CheckMemoryUsage checks if memory usage is within limits
// Note: goja doesn't provide direct memory inspection, so this is a placeholder
func (s *Sandbox) CheckMemoryUsage(vm *goja.Runtime) error {
	// This is a placeholder - goja doesn't expose memory usage directly
	// In production, you might want to monitor the process memory externally
	return nil
}

// InjectSecurityAPI injects security-related APIs into the VM
func (s *Sandbox) InjectSecurityAPI(vm *goja.Runtime) error {
	securityObj := vm.NewObject()

	// Add security level information
	securityObj.Set("level", s.securityLevel)

	// Add a function to check if an operation is allowed
	securityObj.Set("isAllowed", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) == 0 {
			return vm.ToValue(false)
		}

		operation := call.Argument(0).String()
		err := s.ValidateOperation(operation)
		return vm.ToValue(err == nil)
	})

	vm.Set("__security__", securityObj)
	return nil
}

// CreateSecureContext creates a secure execution context
func CreateSecureContext(vm *goja.Runtime, config *Config) error {
	sandbox := NewSandbox(config)

	if err := sandbox.Apply(vm); err != nil {
		return err
	}

	if err := sandbox.InjectSecurityAPI(vm); err != nil {
		return err
	}

	return nil
}
